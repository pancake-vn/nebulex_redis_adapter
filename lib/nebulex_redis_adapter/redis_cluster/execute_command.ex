defmodule NebulexRedisAdapter.RedisCluster.ExecuteCommand do
  alias NebulexRedisAdapter.RedisCluster.{CommandSpec}

  alias NebulexRedisAdapter.Command

  @moduledoc """
  Module responsible for executing Redis commands.
  """
  alias NebulexRedisAdapter.RedisCluster

  def execute(adapter_meta, %CommandSpec{command: command, key: key}, opts) do
    Command.exec(adapter_meta, command, key, opts)
  end

  ## Note
  ## This function is called when the command is a pipeline or multiple keys

  def execute(adapter_meta, commands, opts) when is_list(commands) do
    %{mode: :redis_cluster, keyslot: keyslot} = adapter_meta

    cluster_shards = find_cluster_shards(adapter_meta, opts)

    values =
      Enum.reduce(commands, %{}, fn spec, acc ->
        case spec do
          %CommandSpec{commands: sub_commands, key: key}
          when sub_commands == nil or sub_commands == [] ->
            hash_slot = RedisCluster.hash_slot(key, keyslot)
            {:"$hash_slot", slot} = hash_slot

            shard =
              Enum.find(cluster_shards, fn {_, start, stop} -> slot >= start and slot <= stop end)

            shard_commands = Map.get(acc, shard, []) ++ [spec]
            Map.put(acc, shard, shard_commands)

          %CommandSpec{commands: sub_commands} ->
            Map.put(acc, spec, sub_commands)
        end
      end)
      |> Task.async_stream(fn
        {%CommandSpec{} = command_spec, sub_commands} ->
          opts = Keyword.put(opts, :cluster_shards, cluster_shards)
          {:ok, values} = execute(adapter_meta, sub_commands, opts)
          [{command_spec, values}]

        {shard, shard_commands} ->
          pipeline_commands =
            Enum.map(shard_commands, fn %CommandSpec{command: command} -> command end)

          opts = Keyword.put(opts, :shard, shard)
          {:ok, values} = Command.pipeline(adapter_meta, pipeline_commands, shard, opts)
          Enum.zip(shard_commands, values)
      end)
      |> Stream.flat_map(fn {:ok, values} -> values end)
      |> Enum.sort_by(fn {%CommandSpec{index: index}, _value} -> index end)
      |> Enum.map(fn {_, value} -> value end)

    {:ok, values}
  end

  defp find_cluster_shards(adapter_meta, opts) do
    if cluster_shards = opts[:cluster_shards],
      do: cluster_shards,
      else:
        adapter_meta[:cluster_shards_tab]
        |> :ets.lookup(:cluster_shards)
  end
end
