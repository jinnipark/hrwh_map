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
-export([insert_server/3, insert_server/4, get_server/2, get_all_servers/1, delete_server/2]).
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
-type load() :: number().

-spec stop(server()) ->
    ok.
stop(Server) ->
  gen_server:cast(Server, {?MODULE, stop}).

-spec insert_server(server(), ServerName :: name(), weight()) ->
    ok.
insert_server(Server, ServerName, Weight) ->
  insert_server(Server, ServerName, Weight, 0).

-spec insert_server(server(), ServerName :: name(), weight(), OffLoad :: load()) ->
    ok.
insert_server(Server, ServerName, Weight, OffLoad) ->
  gen_server:call(Server, {?MODULE, insert_server, ServerName, Weight, OffLoad}).

-spec get_server(server(), ServerName :: name()) ->
    {ok, {ServerName :: name(), weight(), Offload :: load()}} |
    {error, not_found}.
get_server(Server, ServerName) ->
  gen_server:call(Server, {?MODULE, get_server, ServerName}).

-spec get_all_servers(server()) ->
    [{ServerName :: name(), weight(), OffLoad :: load()}].
get_all_servers(Server) ->
  gen_server:call(Server, {?MODULE, get_all_servers}).

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

init(_Args) ->
  Table = ets:new(?MODULE, []),
  random:seed(os:timestamp()),
  {ok, #?STATE{table=Table}}.

handle_call({?MODULE, insert_server, ServerName, Weight, OffLoad}, _From, State) ->
  S = xxhash:hash32(ServerName),
  ets:insert(State#?STATE.table, {ServerName, S, Weight, OffLoad}),
  {reply, ok, State};
handle_call({?MODULE, get_server, ServerName}, _From, State) ->
  case ets:lookup(State#?STATE.table, ServerName) of
    [{ServerName, _, Weight, OffLoad}] ->
      {reply, {ok, {ServerName, Weight, OffLoad}}, State};
    [] ->
      {reply, {error, not_found}, State}
  end;
handle_call({?MODULE, get_all_servers}, _From, State) ->
  {reply, ets:tab2list(State#?STATE.table), State};
handle_call({?MODULE, delete_server, ServerName}, _From, State) ->
  ets:delete(State#?STATE.table, ServerName),
  {reply, ok, State};
handle_call({?MODULE, select_server, ObjName}, _From, State) ->
  D = xxhash:hash32(ObjName),
  % Find a candidate and an alternative among all the servers.
  {{ServerName, _, _, OffLoad}, {AltServerName, _, _}} =
    ets:foldl(
      fun({ServerName1, S1, Weight1, OffLoad1},
            {{ServerName0, S0, W0, OffLoad0}, {AltServerName, AltS, AltW}}) ->
        W1 = weight(S1, D) * Weight1,
        case is_preferred(W1, S1, W0, S0) of
          true ->
            % New most preferred server found.
            % Old most preferred server becomes alternative if not offloading.
            AltServer =
              case OffLoad0 == 0 of
                true -> {ServerName0, S0, W0};
                _ -> {AltServerName, AltS, AltW}
              end,
            {{ServerName1, S1, W1, OffLoad1}, AltServer};
          _ ->
            % Check to replace alternative.
            AltServer =
              case (OffLoad1 == 0) andalso is_preferred(W1, S1, AltW, AltS) of
                true -> {ServerName1, S1, W1};
                _ -> {AltServerName, AltS, AltW}
              end,
            {{ServerName0, S0, W0, OffLoad0}, AltServer}
        end
      end,
      {{undefined, 0, 0, 0}, {undefined, 0, 0}},
      State#?STATE.table
    ),
  % Roll a dice to use the candidate or the alternative.
  Result =
    case random:uniform() of
      R when R >= OffLoad -> ServerName;
      _ -> AltServerName
    end,
  {reply, Result, State};
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

-define(INT32_MAX, 2147483648).

weight(S, D) ->
  (1103515245*((1103515245*S+12345) bxor D) + 12345) rem ?INT32_MAX.

is_preferred(W1, S1, W0, S0) when (W1 > W0) orelse ((W1 == W0) andalso (S1 > S0)) ->
  true;
is_preferred(_, _, _, _) ->
  false.
