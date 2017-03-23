module Slate.Engine.Engine
    exposing
        ( QueryStateId
        , Config
        , Model
        , Msg
        , init
        , update
        , executeQuery
        , refreshQuery
        , disposeQuery
        , cascadingDeleteOccurred
        , importQueryState
        , exportQueryState
        )

{-|
    Slate Query Engine.

@docs QueryStateId, Config , Model , Msg , init , update , executeQuery , refreshQuery , disposeQuery , cascadingDeleteOccurred, importQueryState , exportQueryState
-}

import String exposing (..)
import StringUtils exposing (..)
import Dict exposing (..)
import Set exposing (..)
import Json.Decode as JD exposing (..)
import Json.Encode as JE exposing (..)
import Utils.Json as JsonU exposing ((///), (<||))
import List.Extra as ListE exposing (..)
import Regex exposing (HowMany(All, AtMost))
import Utils.Regex as RegexU
import DebugF exposing (..)
import Slate.Engine.Query exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Mutation exposing (..)
import Slate.Common.Query exposing (..)
import Utils.Ops exposing (..)
import Utils.Tuple exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Postgres exposing (..)
import ParentChildUpdate
import Retry exposing (FailureTagger)


-- API


{-|
    QueryState id.
-}
type alias QueryStateId =
    QueryId


{-|
    Slate Engine configuration.
-}
type alias Config msg =
    { debug : Bool
    , logTagger : ( LogLevel, ( QueryStateId, String ) ) -> msg
    , errorTagger : ( ErrorType, ( QueryStateId, String ) ) -> msg
    , eventProcessingErrorTagger : ( String, String ) -> msg
    , completionTagger : QueryStateId -> msg
    , routeToMeTagger : Msg -> msg
    , queryBatchSize : Int
    }


{-|
    Engine Model.
-}
type alias Model msg =
    { nextId : QueryStateId
    , queryStates : Dict QueryStateId (QueryState msg)
    , retryModel : Retry.Model Msg
    }


{-|
    Engine Msg.
-}
type Msg
    = Nop
    | ConnectError QueryStateId ( ConnectionId, String )
    | Connect QueryStateId ConnectionId
    | ConnectionLost QueryStateId ( ConnectionId, String )
    | DisconnectError QueryStateId ( ConnectionId, String )
    | Disconnect QueryStateId ConnectionId
    | Events QueryStateId ( ConnectionId, List String )
    | QueryError QueryStateId ( ConnectionId, String )
    | RetryConnectCmd Int Msg (Cmd Msg)
    | RetryModule (Retry.Msg Msg)


{-|
    Initial model for the Engine.
-}
initModel : ( Model msg, List (Cmd msg) )
initModel =
    ( { nextId = 0
      , queryStates = Dict.empty
      , retryModel = Retry.initModel
      }
    , []
    )


{-|
    Initialize command processor
-}
init : Config msg -> ( Model msg, Cmd msg )
init config =
    let
        ( model, cmds ) =
            initModel
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


{-|
    Engine's update function.
-}
update : Config msg -> Msg -> Model msg -> ( ( Model msg, Cmd Msg ), List msg )
update config msg model =
    let
        logMsg queryStateId message =
            config.logTagger ( LogLevelInfo, ( queryStateId, message ) )

        nonFatal queryStateId error =
            config.errorTagger ( NonFatalError, ( queryStateId, error ) )

        fatal queryStateId error =
            config.errorTagger ( FatalError, ( queryStateId, error ) )

        updateRetry =
            ParentChildUpdate.updateChildParent (Retry.update retryConfig) (update config) .retryModel RetryModule (\model retryModel -> { model | retryModel = retryModel })
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            ConnectError queryStateId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "ConnectError" ( queryStateId, connectionId, error )
                in
                    connectionFailure config model queryStateId error

            ConnectionLost queryStateId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "ConnectLost" ( queryStateId, connectionId, error )
                in
                    connectionFailure config model queryStateId error

            Connect queryStateId connectionId ->
                let
                    l =
                        (debugLog config) "Connect" ( queryStateId, connectionId )

                    queryState =
                        getQueryState queryStateId model

                    ( newModel, cmd ) =
                        startQuery config model queryStateId connectionId
                in
                    ( newModel ! [ cmd ], [] )

            DisconnectError queryStateId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "DisconnectError" ( queryStateId, connectionId, error )

                    queryState =
                        getQueryState queryStateId model
                in
                    connectionFailure config model queryStateId error

            Disconnect queryStateId connectionId ->
                let
                    l =
                        (debugLog config) "Disconnect" ( queryStateId, connectionId )
                in
                    ( model ! [], [] )

            Events queryStateId ( connectionId, eventStrs ) ->
                let
                    l =
                        (debugFLog config) "Events" ( queryStateId, connectionId, eventStrs )

                    ( updatedModel, msgs ) =
                        processEvents config model queryStateId eventStrs

                    queryState =
                        getQueryState queryStateId model

                    goodQueryState =
                        queryState.badQueryState == False

                    startOfQuery =
                        eventStrs == []

                    ( finalModel, cmd ) =
                        goodQueryState
                            ? ( case startOfQuery of
                                    True ->
                                        startQuery config updatedModel queryStateId connectionId

                                    False ->
                                        ( updatedModel, nextQuery queryStateId connectionId )
                              , model ! []
                              )

                    {- handle end of query -}
                    endOfQuery =
                        cmd == Cmd.none

                    ( finalMsgs, finalCmd ) =
                        endOfQuery ? ( ( List.append msgs [ config.completionTagger queryStateId ], Postgres.disconnect (DisconnectError queryStateId) (Disconnect queryStateId) connectionId False ), ( msgs, cmd ) )
                in
                    ( finalModel ! [ finalCmd ], finalMsgs )

            QueryError queryStateId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "QueryError" ( queryStateId, connectionId, error )

                    queryState =
                        getQueryState queryStateId model
                in
                    connectionFailure config model queryStateId error

            RetryConnectCmd retryCount failureMsg cmd ->
                let
                    l =
                        (debugLog config) "RetryConnectCmd" ( retryCount, failureMsg, cmd )

                    parentMsg =
                        case failureMsg of
                            ConnectError queryStateId ( connectionId, error ) ->
                                nonFatal queryStateId ("Connection Error:" +-+ error +-+ "Connection Retry:" +-+ retryCount)

                            _ ->
                                Debug.crash "BUG -- Should never get here"
                in
                    ( model ! [ cmd ], [ parentMsg ] )

            RetryModule msg ->
                updateRetry msg model


{-|
   Execute Slate Query.
-}
executeQuery : Config msg -> DbConnectionInfo -> Model msg -> Maybe String -> Query msg -> List String -> Result (List String) ( Model msg, Cmd msg, Int )
executeQuery config dbInfo model additionalCriteria query rootIds =
    let
        templateResult =
            buildQueryTemplate query
    in
        templateResult
            |??>
                (\templates ->
                    let
                        queryStateId =
                            model.nextId

                        rootEntityName =
                            case query of
                                Node nodeQuery _ ->
                                    nodeQuery.schema.entityName

                                Leaf nodeQuery ->
                                    nodeQuery.schema.entityName

                        queryState =
                            { rootEntityName = rootEntityName
                            , badQueryState = False
                            , currentTemplate = 0
                            , templates = templates
                            , rootIds = rootIds
                            , ids = Dict.empty
                            , additionalCriteria = additionalCriteria
                            , maxIds = Dict.empty
                            , firstTemplateWithDataMaxId = -1
                            , msgDict = buildMsgDict query
                            , first = True
                            }

                        ( newModel, cmd ) =
                            connectToDb config dbInfo { model | nextId = model.nextId + 1, queryStates = Dict.insert queryStateId queryState model.queryStates } queryStateId
                    in
                        ( newModel, cmd, queryStateId )
                )


{-|
    Refresh an existing Slate Query, i.e. process events since the last `executeQuery` or `refreshQuery`.
-}
refreshQuery : Config msg -> DbConnectionInfo -> Model msg -> QueryStateId -> Result String ( Model msg, Cmd msg )
refreshQuery config dbInfo model queryStateId =
    Dict.get queryStateId model.queryStates
        |?> (\queryState ->
                let
                    newQueryState =
                        { queryState | currentTemplate = 0, firstTemplateWithDataMaxId = -1 }

                    ( newModel, cmd ) =
                        connectToDb config dbInfo { model | queryStates = Dict.insert queryStateId newQueryState model.queryStates } queryStateId
                in
                    Ok ( newModel, cmd )
            )
        ?= Err ("Invalid QueryStateId:" +-+ queryStateId)


{-|
    Stop managing specified `Query`. Afterwards, the `queryStateId` will no longer be valid.
-}
disposeQuery : Model msg -> QueryStateId -> Model msg
disposeQuery model queryStateId =
    { model | queryStates = Dict.remove queryStateId model.queryStates }


{-|
    Inform the engine that the parent of this entity was deleted so it can remove the specified EntityId from QueryState ids.
    This is necessary since the Engine proactively loads ids for children as a parent's messages are being processed.
    But when a parent entity is destroyed, we don't want to load all if it's children anymore.
-}
cascadingDeleteOccurred : Model msg -> QueryStateId -> CascadingDelete -> Model msg
cascadingDeleteOccurred model queryStateId cascadingDelete =
    let
        queryState =
            getQueryState queryStateId model

        deleteIds : EntityName -> List RelationshipId -> QueryState msg -> QueryState msg
        deleteIds entityName relationshipIds queryState =
            let
                maybeEngineIds =
                    Dict.get entityName queryState.ids
            in
                maybeEngineIds
                    |?> (\engineIds -> { queryState | ids = Dict.insert entityName (relationshipIds |> List.foldl (\relationshipId -> Set.remove relationshipId) engineIds) queryState.ids })
                    ?= queryState

        newQueryState : QueryState msg
        newQueryState =
            cascadingDelete
                |> List.foldl (\( entityName, relationshipIds ) -> deleteIds entityName relationshipIds) queryState
    in
        { model | queryStates = Dict.insert queryStateId newQueryState model.queryStates }


{-|
    Create a JSON String for saving the specified `QueryState`.

    This is useful for caching a query or saving for a subsequent execution of your App.
-}
exportQueryState : Model msg -> QueryStateId -> Result String String
exportQueryState model queryStateId =
    Dict.get queryStateId model.queryStates
        |?> (\queryState -> Ok <| queryStateEncode queryState)
        ?= Err ("Invalid QueryStateId:" +-+ queryStateId)


{-|
    Recreate a previously saved `QueryState` from the specified JSON String.
-}
importQueryState : Query msg -> Model msg -> String -> Result String (Model msg)
importQueryState query model json =
    let
        templateResult =
            buildQueryTemplate query
    in
        (queryStateDecode (buildMsgDict query) json)
            |??>
                (\queryState ->
                    let
                        queryStateId =
                            model.nextId
                    in
                        { model | nextId = model.nextId + 1, queryStates = Dict.insert queryStateId queryState model.queryStates }
                )



-- PRIVATE API


debugLog : Config msg -> String -> a -> a
debugLog config prefix =
    config.debug ? ( Debug.log prefix, identity )


debugFLog : Config msg -> String -> a -> a
debugFLog config prefix =
    config.debug ? ( DebugF.log prefix, identity )


retryConfig : Retry.Config Msg
retryConfig =
    { retryMax = 3
    , delayNext = Retry.constantDelay 5000
    , routeToMeTagger = RetryModule
    }


type alias QueryState msg =
    { first : Bool
    , rootEntityName : EntityName
    , badQueryState : Bool
    , currentTemplate : Int
    , templates : List String
    , rootIds : List EntityId
    , ids : Dict EntityName (Set EntityId)
    , additionalCriteria : Maybe String
    , maxIds : Dict EntityId Int
    , firstTemplateWithDataMaxId : Int
    , msgDict : MsgDict msg
    }


queryStateEncode : QueryState msg -> String
queryStateEncode queryState =
    JE.encode 0 <|
        JE.object
            [ ( "first", JE.bool queryState.first )
            , ( "rootEntityName", JE.string queryState.rootEntityName )
            , ( "badQueryState", JE.bool queryState.badQueryState )
            , ( "currentTemplate", JE.int queryState.currentTemplate )
            , ( "templates", JE.list <| List.map JE.string queryState.templates )
            , ( "rootIds", JE.list <| List.map JE.string queryState.rootIds )
            , ( "ids", JsonU.encDict JE.string (JE.list << List.map JE.string << Set.toList) queryState.ids )
            , ( "additionalCriteria", JsonU.encMaybe JE.string queryState.additionalCriteria )
            , ( "maxIds", JsonU.encDict JE.string JE.int queryState.maxIds )
            , ( "firstTemplateWithDataMaxId", JE.int queryState.firstTemplateWithDataMaxId )
            ]


queryStateDecode : MsgDict msg -> String -> Result String (QueryState msg)
queryStateDecode msgDict json =
    JD.decodeString
        ((JD.succeed QueryState)
            <|| (field "first" JD.bool)
            <|| (field "rootEntityName" JD.string)
            <|| (field "badQueryState" JD.bool)
            <|| (field "currentTemplate" JD.int)
            <|| (field "templates" <| JD.list JD.string)
            <|| (field "rootIds" <| JD.list JD.string)
            <|| (field "ids" <| JsonU.decConvertDict Set.fromList JD.string (JD.list JD.string))
            <|| (field "additionalCriteria" <| JD.maybe JD.string)
            <|| (field "maxIds" <| JsonU.decDict JD.string JD.int)
            <|| (field "firstTemplateWithDataMaxId" JD.int)
            <|| JD.succeed msgDict
        )
        json


getQueryState : QueryStateId -> Model msg -> QueryState msg
getQueryState queryStateId model =
    case Dict.get queryStateId model.queryStates of
        Just queryState ->
            queryState

        Nothing ->
            Debug.crash <| "Query Id: " ++ (toString queryStateId) ++ " is not in model: " ++ (toString model)


connectionFailure : Config msg -> Model msg -> QueryStateId -> String -> ( ( Model msg, Cmd Msg ), List msg )
connectionFailure config model queryStateId error =
    let
        queryState =
            getQueryState queryStateId model
    in
        ( model ! [], [ config.errorTagger ( NonFatalError, ( queryStateId, error ) ) ] )


templateReplace : List ( String, String ) -> String -> String
templateReplace =
    parametricReplace "{{" "}}"


quoteList : List String -> List String
quoteList =
    List.map (\s -> "'" ++ s ++ "'")


startQuery : Config msg -> Model msg -> QueryStateId -> ConnectionId -> ( Model msg, Cmd Msg )
startQuery config model queryStateId connectionId =
    let
        queryState =
            getQueryState queryStateId model

        maybeTemplate =
            List.head <| (List.drop queryState.currentTemplate queryState.templates)

        firstTemplate =
            queryState.currentTemplate == 0

        haveFirstTemplateMaxId =
            queryState.firstTemplateWithDataMaxId /= -1

        maxIdColumn =
            haveFirstTemplateMaxId ? ( "", ", q.max" )

        maxIdSQLClause =
            haveFirstTemplateMaxId ? ( "", "CROSS JOIN (SELECT MAX(id) FROM events) AS q\n" )

        firstTemplateWithDataMaxCriteria =
            haveFirstTemplateMaxId ? ( "AND id < " ++ (toString queryState.firstTemplateWithDataMaxId), "" )

        entityIds : Dict String (Set String)
        entityIds =
            firstTemplate ? ( Dict.insert queryState.rootEntityName (Set.fromList queryState.rootIds) Dict.empty, queryState.ids )

        lastMaxId =
            toString (queryState.first ? ( -1, (ListE.foldl1 max <| Dict.values queryState.maxIds) ?= -1 ))

        updateQueryState model queryState =
            { model | queryStates = Dict.insert queryStateId queryState model.queryStates }
    in
        maybeTemplate
            |?> (\template ->
                    let
                        sqlTemplate =
                            templateReplace
                                [ ( "additionalCriteria", queryState.additionalCriteria ?= "1=1" )
                                , ( "firstTemplateWithDataMaxCriteria", firstTemplateWithDataMaxCriteria )
                                , ( "maxIdColumn", maxIdColumn )
                                , ( "maxIdSQLClause", maxIdSQLClause )
                                , ( "lastMaxId", lastMaxId )
                                ]
                                template

                        replace entityType ids =
                            let
                                entityIdClause =
                                    (ids == []) ? ( "1=1", "event #>> '{entityId}' IN (" ++ (String.join ", " <| quoteList ids) ++ ")" )
                            in
                                templateReplace [ ( entityType ++ "-entityIds", entityIdClause ) ]

                        sqlWithEntityIds =
                            List.foldl (\( type_, ids ) template -> replace type_ ids template) sqlTemplate (secondMap Set.toList <| Dict.toList entityIds)

                        sql =
                            RegexU.replace All "\\{\\{.+?\\-entityIds\\}\\}" (RegexU.simpleReplacer "1!=1") sqlWithEntityIds

                        ll =
                            (debugFLog config) "sql" sql
                    in
                        ( updateQueryState model { queryState | currentTemplate = queryState.currentTemplate + 1 }
                        , Postgres.query (QueryError queryStateId) (Events queryStateId) connectionId sql config.queryBatchSize
                        )
                )
            ?= ( updateQueryState model { queryState | first = False }, Cmd.none )


processEvents : Config msg -> Model msg -> QueryStateId -> List String -> ( Model msg, List msg )
processEvents config model queryStateId eventStrs =
    let
        queryState =
            getQueryState queryStateId model

        eventNotInDict =
            "Event not in message dictionary:" +-+ queryState.msgDict

        eventError : String -> List msg -> String -> ( QueryState msg, List msg )
        eventError eventStr msgs error =
            ( { queryState | badQueryState = True }, config.eventProcessingErrorTagger ( eventStr, error ) :: msgs )

        ( newQueryState, msgs ) =
            List.foldl
                (\eventStr ( queryState, msgs ) ->
                    let
                        eventRecordDecoded =
                            decodeString eventRecordDecoder eventStr
                    in
                        eventRecordDecoded
                            |??>
                                (\eventRecord ->
                                    let
                                        event =
                                            eventRecord.event

                                        resultRecordId =
                                            toInt eventRecord.id

                                        entityName =
                                            getEntityName event ??= (\error -> Debug.crash ("Program bug: Cannot get entityName. Error:" +-+ error))

                                        eventType =
                                            event |> getEventType |> eventTypeToComparable
                                    in
                                        resultRecordId
                                            |??>
                                                (\recordId ->
                                                    Dict.get ( entityName, eventType ) queryState.msgDict
                                                        |?> (\{ tagger, maybeRelationshipEntityName } ->
                                                                let
                                                                    newMsgs =
                                                                        (tagger queryStateId eventRecord) :: msgs

                                                                    firstTemplateWithDataMaxId =
                                                                        max queryState.firstTemplateWithDataMaxId ((Result.toMaybe <| toInt <| eventRecord.max ?= "-1") ?= -1)
                                                                in
                                                                    {- add relationship entities to ids dictionary for next SQL query -}
                                                                    maybeRelationshipEntityName
                                                                        |?> (\relationshipEntityName ->
                                                                                let
                                                                                    currentEntityMaxId =
                                                                                        (Dict.get relationshipEntityName queryState.maxIds) ?= -1

                                                                                    entityMaxId =
                                                                                        max currentEntityMaxId recordId

                                                                                    ids =
                                                                                        Dict.get relationshipEntityName queryState.ids ?= Set.empty
                                                                                in
                                                                                    getRelationshipId event
                                                                                        |??>
                                                                                            (\relationshipId ->
                                                                                                ( { queryState
                                                                                                    | firstTemplateWithDataMaxId = firstTemplateWithDataMaxId
                                                                                                    , ids = Dict.insert relationshipEntityName (Set.insert relationshipId ids) queryState.ids
                                                                                                    , maxIds = Dict.insert relationshipEntityName entityMaxId queryState.maxIds
                                                                                                  }
                                                                                                , newMsgs
                                                                                                )
                                                                                            )
                                                                                        ??= (\_ -> ( queryState, newMsgs ))
                                                                            )
                                                                        ?= ( { queryState | firstTemplateWithDataMaxId = firstTemplateWithDataMaxId }
                                                                           , newMsgs
                                                                           )
                                                            )
                                                        ?= eventError eventStr msgs eventNotInDict
                                                )
                                            ??= (\_ -> eventError eventStr msgs "Corrupt Event Record -- Invalid Id")
                                )
                            ??= (\decodingErr -> eventError eventStr msgs decodingErr)
                )
                ( queryState, [] )
                eventStrs
    in
        ( { model | queryStates = Dict.insert queryStateId newQueryState model.queryStates }, List.reverse msgs )


nextQuery : QueryStateId -> ConnectionId -> Cmd Msg
nextQuery queryStateId connectionId =
    Postgres.moreQueryResults (QueryError queryStateId) (Events queryStateId) connectionId


connectToDbCmd : Config msg -> DbConnectionInfo -> Model msg -> QueryStateId -> FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectToDbCmd config dbInfo model queryStateId failureTagger =
    Postgres.connect
        failureTagger
        (Connect queryStateId)
        (ConnectionLost queryStateId)
        dbInfo.timeout
        dbInfo.host
        dbInfo.port_
        dbInfo.database
        dbInfo.user
        dbInfo.password


connectToDb : Config msg -> DbConnectionInfo -> Model msg -> QueryStateId -> ( Model msg, Cmd msg )
connectToDb config dbInfo model queryStateId =
    let
        ( retryModel, retryCmd ) =
            Retry.retry retryConfig model.retryModel (ConnectError queryStateId) RetryConnectCmd (connectToDbCmd config dbInfo model queryStateId)
    in
        { model | retryModel = retryModel } ! [ Cmd.map config.routeToMeTagger retryCmd ]
