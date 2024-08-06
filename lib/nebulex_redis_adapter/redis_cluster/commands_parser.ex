defmodule NebulexRedisAdapter.RedisCluster.CommandSpec do
  @doc """
  Command specification.

  This struct is used to store the command specification.

  Attributes:
    * `index` - the index of the command in the pipeline or command multiple keys
    * `command` - the command to be executed
    * `key` - the key to be used in the command
    * `commands` - the list of commands to be executed in a pipeline
  """
  @type t :: %__MODULE__{
          index: integer | [integer],
          command: list,
          key: String.t(),
          commands: [NebulexRedisAdapter.RedisCluster.CommandSpec.t()]
        }
  defstruct index: nil,
            command: nil,
            key: nil,
            commands: nil
end

defmodule NebulexRedisAdapter.RedisCluster.CommandParser do
  @doc """
  Parses the commands to be executed.
  """

  alias NebulexRedisAdapter.RedisCluster.CommandSpec

  @commands_spec %{
    "BITOP" => %{},
    "BITCOUNT" => %{},
    "PSETEX" => %{},
    "LLEN" => %{},
    "ZRANGEBYSCORE" => %{},
    "LINSERT" => %{},
    "SETRANGE" => %{},
    "DEL" => %{},
    "ZRANDMEMBER" => %{},
    "ZREM" => %{},
    "ZPOPMIN" => %{},
    "ZREVRANGE" => %{},
    "HLEN" => %{},
    "ZRANGEBYLEX" => %{},
    "DECRBY" => %{},
    "EXISTS" => %{},
    "INCRBYFLOAT" => %{},
    "RPUSHX" => %{},
    "ZINTERSTORE" => %{},
    "HGETALL" => %{},
    "MSET" => %{},
    "RPOP" => %{},
    "ZREVRANGEBYLEX" => %{},
    "LINDEX" => %{},
    "LRANGE" => %{},
    "RPOPLPUSH" => %{},
    "SET" => %{},
    "SETEX" => %{},
    "SREM" => %{},
    "HEXISTS" => %{},
    "ZUNIONSTORE" => %{},
    "ZRANGE" => %{},
    "ZREVRANK" => %{},
    "GETSET" => %{},
    "ZINCRBY" => %{},
    "ZLEXCOUNT" => %{},
    "SISMEMBER" => %{},
    "STRLEN" => %{},
    "BITPOS" => %{},
    "HMSET" => %{},
    "SADD" => %{},
    "PING" => %{},
    "SCAN" => %{},
    "HINCRBYFLOAT" => %{},
    "EXPIRE" => %{},
    "PEXPIRE" => %{},
    "INCR" => %{},
    "ZREMRANGEBYRANK" => %{},
    "RTRIM" => %{},
    "HSETNX" => %{},
    "INCRBY" => %{},
    "HINCRBY" => %{},
    "HVALS" => %{},
    "GETBIT" => %{},
    "HSET" => %{},
    "HDEL" => %{},
    "ZSCAN" => %{},
    "HKEYS" => %{},
    "APPEND" => %{},
    "SETNX" => %{},
    "TTL" => %{},
    "DECR" => %{},
    "LPOP" => %{},
    "HGET" => %{},
    "ZCOUNT" => %{},
    "ZCARD" => %{},
    "HMGET" => %{},
    "LPUSH" => %{},
    "ZADD" => %{},
    "ZSCORE" => %{},
    "LREM" => %{},
    "MGET" => %{},
    "ZPOPMAX" => %{},
    "SETBIT" => %{},
    "ZREMRANGEBYSCORE" => %{},
    "SMEMBERS" => %{},
    "GET" => %{},
    "ZREMRANGEBYLEX" => %{},
    "ZREVRANGEBYSCORE" => %{},
    "RPUSH" => %{},
    "SPOP" => %{}
  }
  @commands_spec
  |> Enum.concat(
    Enum.map(@commands_spec, fn {command, spec} -> {String.downcase(command), spec} end)
  )
  |> Enum.map(fn {command, _spec} ->
    def command_spec([unquote(command) | tail] = full_command, opts) do
      command_index = Keyword.get(opts, :index, 0)
      # :command | :pipeline
      command_source = Keyword.get(opts, :source, :command)

      cond do
        unquote(command) in ["MGET", "MSET", "mget", "mset"] ->
          {keys_values, opts} =
            case tail do
              [keys_values | nonexisting] when nonexisting in ["NONEXISTING", "nonexisting"] ->
                {keys_values, ["NONEXISTING"]}

              keys_values ->
                {keys_values, []}
            end

          {chunk_every, parsed_command} =
            case unquote(command) do
              "MGET" -> {1, "GET"}
              "MSET" -> {2, "SET"}
              "mget" -> {1, "GET"}
              "mset" -> {2, "SET"}
            end

          command_specs =
            keys_values
            |> Enum.chunk_every(chunk_every)
            |> Enum.with_index()
            |> Enum.map(fn
              {
                [key, _value] = pair,
                index
              } ->
                %CommandSpec{index: [index], command: [parsed_command] ++ pair ++ opts, key: key}

              {[key], index} ->
                %CommandSpec{index: index, command: [parsed_command] ++ [key] ++ opts, key: key}
            end)

          case command_source do
            :pipeline ->
              %CommandSpec{index: command_index, command: nil, key: nil, commands: command_specs}

            _ ->
              command_specs
          end

        true ->
          [key | _] = tail
          %CommandSpec{index: command_index, command: full_command, key: key}
      end
    end
  end)

  def command_spec(command) do
    raise ArgumentError, "Command not supported #{inspect(command)}"
  end

  def pipline_spec(commands),
    do:
      commands
      |> Enum.with_index()
      |> Enum.map(fn {command, index} ->
        command_spec(command, index: index, source: :pipeline)
      end)
end
