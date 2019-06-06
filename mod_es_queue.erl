%%%-------------------------------------------------------------------
%%% @author Linus Schoemaker <linus@jake>
%%% @copyright (C) 2019, Linus Schoemaker
%%% @doc
%%%
%%% @end
%%% Created : 24 Jan 2019 by Linus Schoemaker <linus@jake>
%%%-------------------------------------------------------------------
-module(mod_es_queue).

-mod_title("ElasticSearch bulk insert queue").
-mod_prio(500).
-mod_description("Stores a queue of documents to bulk insert into ElasticSearch").

-include_lib("zotonic.hrl").
-include("deps/erlastic_search/include/erlastic_search.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1, insert/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
start_link(Args) when is_list(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

insert(Index, Type, Id, Document) ->
    gen_server:cast(?MODULE, {insert, {Index, Type, Id, Document}}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: term()} |
                              {ok, State :: term(), Timeout :: timeout()} |
                              {ok, State :: term(), hibernate} |
                              {stop, Reason :: term()} |
                              ignore.
init(_Args) ->
    {ok, #{}, flush_interval()}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
                         {reply, Reply :: term(), NewState :: term()} |
                         {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
                         {reply, Reply :: term(), NewState :: term(), hibernate} |
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
                         {stop, Reason :: term(), NewState :: term()}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: term(), NewState :: term()}.
handle_cast({insert, {_, _, Id, _} = Doc}, Docs) ->
    Queue = maps:put(Id, Doc, Docs),
    case maps:size(Queue) >= queue_length() of
        true ->
            lager:debug("Bulk inserting all documents!"),
            case erlastic_search:bulk_index_docs(#erls_params{}, maps:values(Queue)) of
                {ok, List} ->
                    lager:debug("Ok! ~p~n", [List]);
                {error, Error} ->
                    lager:error("Error bulk inserting documents: ~p~n", [Error])
            end,
            {noreply, #{}, flush_interval()};
        false ->
            {noreply, Queue, flush_interval()}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(timeout, State) ->
    lager:debug("Timeout for ElasticSearch bulk insert queue has expired. Flushing ~p documents now~n", [maps:size(State)]),
    case maps:size(State) > 0 of
        true ->
            case erlastic_search:bulk_index_docs(#erls_params{}, maps:values(State)) of
                {ok, List} ->
                    lager:debug("Ok! ~p~n", [List]);
                {error, Error} ->
                    lager:error("Error bulk inserting documents: ~p~n", [Error])
            end;
        false ->
            noop
    end,
    {noreply, #{}, hibernate}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: term(),
                  Extra :: term()) -> {ok, NewState :: term()} |
                                      {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
                    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

queue_length() ->
    500.

flush_interval() ->
    5000.
