module Slate.Engine.Engine
    exposing
        ( Config
        , Model
        , Msg
        , init
        , update
        , executeQuery
        , refreshQuery
        , disposeQuery
        , cascadingDeleteOccurred
        , importQueryMismatchError
        , importQueryState
        , exportQueryState
        , isGoodQueryState
        )

{-|
    Slate Query Engine.

@docs Config , Model , Msg , init , update , executeQuery , refreshQuery , disposeQuery , cascadingDeleteOccurred, importQueryMismatchError, importQueryState , exportQueryState, isGoodQueryState
-}

import String exposing (..)
import StringUtils exposing (..)
import Dict exposing (..)
import Set exposing (..)
import Json.Decode as JD exposing (..)
import Json.Encode as JE exposing (..)
import Utils.Json as Json exposing ((///), (<||))
import List.Extra as List exposing (..)
import Regex exposing (HowMany(All, AtMost))
import Utils.Regex as RegexU
import DebugF exposing (..)
import Slate.Engine.Query as EngineQuery exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Mutation exposing (..)
import Slate.Common.Query exposing (..)
import Utils.Ops exposing (..)
import Utils.Tuple exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Postgres exposing (ConnectionId, QueryTagger, Sql)
import ParentChildUpdate
import Retry exposing (FailureTagger)


-- API


{-|
    Slate Engine configuration.
-}
type alias Config msg =
    { debug : Bool
    , connectionRetryMax : Int
    , logTagger : ( LogLevel, ( QueryId, String ) ) -> msg
    , errorTagger : ( ErrorType, ( QueryId, String ) ) -> msg
    , eventProcessingErrorTagger : ( String, String ) -> msg
    , completionTagger : QueryId -> msg
    , routeToMeTagger : Msg -> msg
    , queryBatchSize : Int
    }


{-|
    Engine Model.
-}
type alias Model msg =
    { nextId : QueryId
    , queryStates : Dict QueryId (QueryState msg)
    , retryModels : Dict QueryId (Retry.Model Msg)
    }


{-|
    Engine Msg.
-}
type Msg
    = Nop
    | ConnectError QueryId ( ConnectionId, String )
    | Connect QueryId ConnectionId
    | ConnectionLost QueryId ( ConnectionId, String )
    | DisconnectError QueryId ( ConnectionId, String )
    | Disconnect QueryId ConnectionId
    | Events QueryId ( ConnectionId, List String )
    | QueryError QueryId ( ConnectionId, String )
    | RetryConnectCmd Int Msg (Cmd Msg)
    | RetryMsg QueryId (Retry.Msg Msg)


{-|
    Initial model for the Engine.
-}
initModel : ( Model msg, List (Cmd msg) )
initModel =
    ( { nextId = 0
      , queryStates = Dict.empty
      , retryModels = Dict.empty
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
        logMsg queryId message =
            config.logTagger ( LogLevelInfo, ( queryId, message ) )

        nonFatal queryId error =
            config.errorTagger ( NonFatalError, ( queryId, error ) )

        fatal queryId error =
            config.errorTagger ( FatalError, ( queryId, error ) )

        getRetryModel config model queryId =
            Dict.get queryId model.retryModels
                ?!= (\_ -> Debug.crash ("BUG: Cannot find retry model for queryId:" +-+ queryId))

        updateRetry queryId =
            ParentChildUpdate.updateChildParent (Retry.update <| retryConfig config queryId) (update config) (\model -> getRetryModel config model queryId) (RetryMsg queryId) (\model retryModel -> { model | retryModels = Dict.insert queryId retryModel model.retryModels })
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            ConnectError queryId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "ConnectError" ( queryId, connectionId, error )
                in
                    { model | retryModels = Dict.remove queryId model.retryModels }
                        |> (\model -> connectionFailure config model queryId error)

            ConnectionLost queryId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "ConnectLost" ( queryId, connectionId, error )
                in
                    connectionFailure config model queryId error

            Connect queryId connectionId ->
                let
                    l =
                        (debugLog config) "Connect" ( queryId, connectionId )

                    queryState =
                        getQueryState queryId model
                in
                    { model | retryModels = Dict.remove queryId model.retryModels }
                        |> (\model ->
                                startQuery config model queryId connectionId
                                    |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))
                           )

            DisconnectError queryId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "DisconnectError" ( queryId, connectionId, error )

                    queryState =
                        getQueryState queryId model
                in
                    connectionFailure config model queryId error

            Disconnect queryId connectionId ->
                let
                    l =
                        (debugLog config) "Disconnect" ( queryId, connectionId )
                in
                    ( model ! [], [] )

            Events queryId ( connectionId, eventStrs ) ->
                let
                    l =
                        (debugFLog config) "Events" ( queryId, connectionId, eventStrs )

                    ( updatedModel, msgs ) =
                        processEvents config model queryId eventStrs

                    queryState =
                        getQueryState queryId model

                    goodQueryState =
                        queryState.badQueryState == False

                    startOfQuery =
                        eventStrs == []

                    ( finalModel, cmd ) =
                        goodQueryState
                            ? ( case startOfQuery of
                                    True ->
                                        startQuery config updatedModel queryId connectionId

                                    False ->
                                        ( updatedModel, nextQuery queryId connectionId )
                              , model ! []
                              )

                    {- handle end of query -}
                    endOfQuery =
                        cmd == Cmd.none

                    ( finalMsgs, finalCmd ) =
                        endOfQuery ? ( ( List.append msgs [ config.completionTagger queryId ], Postgres.disconnect (DisconnectError queryId) (Disconnect queryId) connectionId False ), ( msgs, cmd ) )
                in
                    ( finalModel ! [ finalCmd ], finalMsgs )

            QueryError queryId ( connectionId, error ) ->
                let
                    l =
                        (debugLog config) "QueryError" ( queryId, connectionId, error )

                    queryState =
                        getQueryState queryId model
                in
                    connectionFailure config model queryId error

            RetryConnectCmd retryCount failureMsg cmd ->
                let
                    l =
                        (debugLog config) "RetryConnectCmd" ( retryCount, failureMsg, cmd )

                    parentMsg =
                        case failureMsg of
                            ConnectError queryId ( connectionId, error ) ->
                                nonFatal queryId ("Connection Error:" +-+ error +-+ "Connection Retry:" +-+ retryCount)

                            _ ->
                                Debug.crash "BUG -- Should never get here"
                in
                    ( model ! [ cmd ], [ parentMsg ] )

            RetryMsg queryId msg ->
                updateRetry queryId msg model


{-|
   Execute Slate Query.
-}
executeQuery : Config msg -> DbConnectionInfo -> Model msg -> Maybe String -> Query msg -> List String -> Result (List String) ( Model msg, Cmd msg, Int )
executeQuery config dbConnectionInfo model additionalCriteria query rootIds =
    buildQueryTemplate query
        |??>
            (\templates ->
                let
                    queryId =
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
                        connectToDb config dbConnectionInfo { model | nextId = model.nextId + 1, queryStates = Dict.insert queryId queryState model.queryStates } queryId
                in
                    ( newModel, cmd, queryId )
            )


{-|
    Refresh an existing Slate Query, i.e. process events since the last `executeQuery` or `refreshQuery`.
-}
refreshQuery : Config msg -> DbConnectionInfo -> Model msg -> QueryId -> Result String ( Model msg, Cmd msg )
refreshQuery config dbConnectionInfo model queryId =
    Dict.get queryId model.queryStates
        |?> (\queryState ->
                let
                    newQueryState =
                        { queryState | currentTemplate = 0, firstTemplateWithDataMaxId = -1 }

                    ( newModel, cmd ) =
                        connectToDb config dbConnectionInfo { model | queryStates = Dict.insert queryId newQueryState model.queryStates } queryId
                in
                    Ok ( newModel, cmd )
            )
        ?= Err ("Invalid QueryId:" +-+ queryId)


{-|
    Stop managing specified `Query`. Afterwards, the `queryId` will no longer be valid.
-}
disposeQuery : Model msg -> QueryId -> Model msg
disposeQuery model queryId =
    { model | queryStates = Dict.remove queryId model.queryStates }


{-|
    Inform the engine that the parent of this entity was deleted so it can remove the specified EntityId from QueryState ids.
    This is necessary since the Engine proactively loads ids for children as a parent's messages are being processed.
    But when a parent entity is destroyed, we don't want to load all if it's children anymore.
-}
cascadingDeleteOccurred : Model msg -> QueryId -> CascadingDelete -> Model msg
cascadingDeleteOccurred model queryId cascadingDelete =
    let
        queryState =
            getQueryState queryId model

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
        { model | queryStates = Dict.insert queryId newQueryState model.queryStates }


{-| Create a JSON String for saving the specified `QueryState`.
-}
exportQueryState : Model msg -> QueryId -> Result String String
exportQueryState model queryId =
    Dict.get queryId model.queryStates
        |?> (\queryState -> Ok <| queryStateEncode queryState)
        ?= Err ("Invalid QueryId:" +-+ queryId)


{-| importQuery query mismatch error
-}
importQueryMismatchError : String
importQueryMismatchError =
    "Import Query Mismatch"


{-| Recreate a previously saved `QueryState` from the specified JSON String.
-}
importQueryState : Query msg -> Model msg -> String -> Result String ( QueryId, Model msg )
importQueryState query model json =
    buildQueryTemplate query
        |??>
            (\templates ->
                (queryStateDecode (buildMsgDict query) json)
                    |??>
                        (\queryState ->
                            (templates == queryState.templates)
                                ? ( model.nextId
                                        |> (\queryId -> Ok ( queryId, { model | nextId = queryId + 1, queryStates = Dict.insert queryId queryState model.queryStates } ))
                                  , Err importQueryMismatchError
                                  )
                        )
                    ??= Err
            )
        ??= (Err << String.join "\n")


{-| check to make sure that query state is good
-}
isGoodQueryState : Model msg -> QueryId -> Bool
isGoodQueryState model queryId =
    getQueryState queryId model
        |> .badQueryState



-- PRIVATE API


debugLog : Config msg -> String -> a -> a
debugLog config prefix =
    config.debug ? ( Debug.log ("*** DEBUG:Query Engine" +-+ prefix), identity )


debugFLog : Config msg -> String -> a -> a
debugFLog config prefix =
    config.debug ? ( DebugF.log prefix, identity )


retryConfig : Config msg -> QueryId -> Retry.Config Msg
retryConfig config queryId =
    { retryMax = config.connectionRetryMax
    , delayNext = Retry.constantDelay 5000
    , routeToMeTagger = RetryMsg queryId
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
            , ( "ids", Json.encDict JE.string (JE.list << List.map JE.string << Set.toList) queryState.ids )
            , ( "additionalCriteria", Json.encMaybe JE.string queryState.additionalCriteria )
            , ( "maxIds", Json.encDict JE.string JE.int queryState.maxIds )
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
            <|| (field "ids" <| Json.decConvertDict Set.fromList JD.string (JD.list JD.string))
            <|| (field "additionalCriteria" <| JD.maybe JD.string)
            <|| (field "maxIds" <| Json.decDict JD.string JD.int)
            <|| (field "firstTemplateWithDataMaxId" JD.int)
            <|| JD.succeed msgDict
        )
        json


getQueryState : QueryId -> Model msg -> QueryState msg
getQueryState queryId model =
    case Dict.get queryId model.queryStates of
        Just queryState ->
            queryState

        Nothing ->
            Debug.crash <| "Query Id: " ++ (toString queryId) ++ " is not in model: " ++ (toString model)


connectionFailure : Config msg -> Model msg -> QueryId -> String -> ( ( Model msg, Cmd Msg ), List msg )
connectionFailure config model queryId error =
    let
        queryState =
            getQueryState queryId model
    in
        ( model ! [], [ config.errorTagger ( NonFatalError, ( queryId, error ) ) ] )


templateReplace : List ( String, String ) -> String -> String
templateReplace =
    parametricReplace "{{" "}}"


quoteList : List String -> List String
quoteList =
    List.map (\s -> "'" ++ s ++ "'")


startQuery : Config msg -> Model msg -> QueryId -> ConnectionId -> ( Model msg, Cmd Msg )
startQuery config model queryId connectionId =
    let
        queryState =
            getQueryState queryId model

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
            toString (queryState.first ? ( -1, (List.foldl1 max <| Dict.values queryState.maxIds) ?= -1 ))

        updateQueryState model queryState =
            { model | queryStates = Dict.insert queryId queryState model.queryStates }
    in
        (List.head <| (List.drop queryState.currentTemplate queryState.templates))
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
                    in
                        ( updateQueryState model { queryState | currentTemplate = queryState.currentTemplate + 1 }
                        , query queryId (QueryError queryId) (Events queryId) connectionId sql config.queryBatchSize
                        )
                )
            ?= ( updateQueryState model { queryState | first = False }, Cmd.none )


query : QueryId -> Postgres.ErrorTagger msg -> QueryTagger msg -> ConnectionId -> Sql -> Int -> Cmd msg
query queryId errorTagger queryTagger connectionId sql =
    Postgres.query errorTagger queryTagger connectionId ("-- Engine:: (QueryId, ConnectionId):" +-+ ( queryId, connectionId ) ++ "\n" ++ sql)


processEvents : Config msg -> Model msg -> QueryId -> List String -> ( Model msg, List msg )
processEvents config model queryId eventStrs =
    let
        queryState =
            getQueryState queryId model

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

                                        resultEntityName =
                                            getEntityName event

                                        eventType =
                                            event |> getEventType |> eventTypeToComparable
                                    in
                                        resultEntityName
                                            |??>
                                                (\entityName ->
                                                    resultRecordId
                                                        |??>
                                                            (\recordId ->
                                                                Dict.get ( entityName, eventType ) queryState.msgDict
                                                                    |?> (\{ tagger, maybeRelationshipEntityName } ->
                                                                            let
                                                                                newMsgs =
                                                                                    (tagger queryId eventRecord) :: msgs

                                                                                firstTemplateWithDataMaxId =
                                                                                    max queryState.firstTemplateWithDataMaxId ((Result.toMaybe <| toInt <| eventRecord.max ?= "-1") ?= -1)

                                                                                entityMaxId entityName =
                                                                                    max (Dict.get entityName queryState.maxIds ?= -1) recordId
                                                                            in
                                                                                {- add relationship entities to ids dictionary for next SQL query -}
                                                                                maybeRelationshipEntityName
                                                                                    |?> (\relationshipEntityName ->
                                                                                            let
                                                                                                ids =
                                                                                                    Dict.get relationshipEntityName queryState.ids ?= Set.empty
                                                                                            in
                                                                                                getRelationshipId event
                                                                                                    |??>
                                                                                                        (\relationshipId ->
                                                                                                            ( { queryState
                                                                                                                | firstTemplateWithDataMaxId = firstTemplateWithDataMaxId
                                                                                                                , ids = Dict.insert relationshipEntityName (Set.insert relationshipId ids) queryState.ids
                                                                                                                , maxIds = Dict.insert relationshipEntityName (entityMaxId relationshipEntityName) queryState.maxIds
                                                                                                              }
                                                                                                            , newMsgs
                                                                                                            )
                                                                                                        )
                                                                                                    ??= (\_ -> ( queryState, newMsgs ))
                                                                                        )
                                                                                    ?= ( { queryState
                                                                                            | firstTemplateWithDataMaxId = firstTemplateWithDataMaxId
                                                                                            , maxIds = Dict.insert entityName (entityMaxId entityName) queryState.maxIds
                                                                                         }
                                                                                       , newMsgs
                                                                                       )
                                                                        )
                                                                    ?= eventError eventStr msgs eventNotInDict
                                                            )
                                                        ??= (\error -> eventError eventStr msgs ("Corrupt Event Record -- Invalid id:" +-+ error))
                                                )
                                            ??= (\error -> eventError eventStr msgs ("Corrupt Event Record -- Missing entityName: Error:" +-+ error))
                                )
                            ??= (\decodingErr -> eventError eventStr msgs decodingErr)
                )
                ( queryState, [] )
                eventStrs
    in
        ( { model | queryStates = Dict.insert queryId newQueryState model.queryStates }, List.reverse msgs )


nextQuery : QueryId -> ConnectionId -> Cmd Msg
nextQuery queryId connectionId =
    Postgres.moreQueryResults (QueryError queryId) (Events queryId) connectionId


connectToDbCmd : Config msg -> DbConnectionInfo -> Model msg -> QueryId -> Retry.FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectToDbCmd config dbConnectionInfo model queryId failureTagger =
    Postgres.connect
        failureTagger
        (Connect queryId)
        (ConnectionLost queryId)
        dbConnectionInfo.timeout
        dbConnectionInfo.host
        dbConnectionInfo.port_
        dbConnectionInfo.database
        dbConnectionInfo.user
        dbConnectionInfo.password


connectToDb : Config msg -> DbConnectionInfo -> Model msg -> QueryId -> ( Model msg, Cmd msg )
connectToDb config dbConnectionInfo model queryId =
    let
        ( retryModel, retryCmd ) =
            Retry.retry (retryConfig config queryId) Retry.initModel (ConnectError queryId) RetryConnectCmd (connectToDbCmd config dbConnectionInfo model queryId)
    in
        { model | retryModels = Dict.insert queryId retryModel model.retryModels } ! [ Cmd.map config.routeToMeTagger retryCmd ]
