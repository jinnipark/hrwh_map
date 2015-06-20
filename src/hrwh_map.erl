%%% --------------------------------------------------------------------------
%%% Highest Random Weight Hash Map
%%%
%%% Ref. http://www.eecs.umich.edu/techreports/cse/96/CSE-TR-316-96.pdf
%%%
%%% @author Sungjin Park
%%% @date Jun 20, 2015
%%% --------------------------------------------------------------------------
-module(hrwh_map).
-author("Sungjin Park <jinni.park@gmail.com>").

-behavior(gen_server).

-export([start/1, start_link/1, stop/1]).
-export([insert_server/3, get_server/2, delete_server/2]).
-export([select_server/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-spec start(Args :: proplists:proplist()) ->
    {ok, pid()} |
    {error, {already_started, pid()} | term()}.
start(Args) ->
  start_server(start, Args).

-spec start_link(Args :: proplists:proplist()) ->
    {ok, pid()} |
    {error, {already_started, pid()} | term()}.
start_link(Args) ->
  start_server(start_link, Args).

start_server(Fun, Args) ->
  case proplists:get_value(name, Args) of
    undefined ->
      gen_server:Fun(?MODULE, Args, []);
    Name ->
      gen_server:Fun(Name, ?MODULE, Args, [])
  end.

-type server() :: pid() | atom().
-type name() :: binary() | string().
-type weight() :: number().

-spec stop(server()) ->
    ok.
stop(Server) ->
  gen_server:cast(Server, {?MODULE, stop}).

-spec insert_server(server(), ServerName :: name(), weight()) ->
    ok.
insert_server(Server, ServerName, Weight) ->
  gen_server:call(Server, {?MODULE, insert_server, ServerName, Weight}).

-spec get_server(server(), ServerName :: name()) ->
    {ok, {ServerName :: name(), weight()}} |
    {error, not_found}.
get_server(Server, ServerName) ->
  gen_server:call(Server, {?MODULE, get_server, ServerName}).

-spec delete_server(server(), ServerName :: name()) ->
    ok.
delete_server(Server, ServerName) ->
  gen_server:call(Server, {?MODULE, delete_server, ServerName}).

-spec select_server(server(), ObjName :: name()) ->
    ServerName :: name().
select_server(Server, ObjName) ->
  gen_server:call(Server, {?MODULE, select_server, ObjName}).

-define(STATE, ?MODULE).
-record(?STATE, {
  table
}).

-define(INT32_MAX, 2147483648).

init(_Args) ->
  Table = ets:new(?MODULE, []),
  {ok, #?STATE{table=Table}}.

handle_call({?MODULE, insert_server, ServerName, Weight}, _From, State) ->
  S = xxhash:hash32(ServerName),
  ets:insert(State#?STATE.table, {ServerName, S, Weight}),
  {reply, ok, State};
handle_call({?MODULE, get_server, ServerName}, _From, State) ->
  case ets:lookup(State#?STATE.table, ServerName) of
    [{ServerName, _, Weight}] -> {reply, {ok, {ServerName, Weight}}, State};
    [] -> {reply, {error, not_found}, State}
  end;
handle_call({?MODULE, delete_server, ServerName}, _From, State) ->
  ets:delete(State#?STATE.table, ServerName),
  {reply, ok, State};
handle_call({?MODULE, select_server, ObjName}, _From, State) ->
  N = xxhash:hash32(ObjName),
  {ServerName, _, _} =
    ets:foldl(
      fun({ServerName1, S1, Weight}, {ServerName0, S0, C0}) ->
        case ((1103515245*((1103515245*S1+12345) bxor N) + 12345) rem ?INT32_MAX) * Weight of
          C1 when C1 > C0 -> {ServerName1, S1, C1};
          C1 when C1 < C0 -> {ServerName0, S0, C0};
          C1 when S1 > S0 -> {ServerName1, S1, C1};
          _ -> {ServerName0, S0, C0}
        end
      end,
      {undefined, 0, 0},
      State#?STATE.table
    ),
  {reply, ServerName, State};
handle_call(_Msg, _From, State) ->
  {reply, {error, unknown}, State}.

handle_cast({?MODULE, stop}, State) ->
  {stop, normal, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  ets:delete(State#?STATE.table).

code_change(_, State, _) ->
  {ok, State}.
