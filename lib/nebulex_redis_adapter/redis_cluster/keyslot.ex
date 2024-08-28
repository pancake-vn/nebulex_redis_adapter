defmodule NebulexRedisAdapter.RedisCluster.Keyslot do
  @moduledoc """
  Default `Nebulex.Adapter.Keyslot` implementation.
  """

  use Nebulex.Adapter.Keyslot

  if Code.ensure_loaded?(CRC) do
    alias NebulexRedisAdapter.Serializer.Serializable

    @impl true
    def hash_slot(key, range)

    def hash_slot("{" <> hash_tags = key, range) do
      case String.split(hash_tags, "}") do
        [key, _] -> do_hash_slot(key, range)
        _ -> do_hash_slot(key, range)
      end
    end

    def hash_slot(key, range) when is_binary(key) do
      do_hash_slot(key, range)
    end

    def hash_slot(key, range) do
      key
      |> Serializable.encode()
      |> do_hash_slot(range)
    end

    defp do_hash_slot(key, range) do
      hash_key =
        case Regex.run(~r/{(.*?)}/, key) do
          [_, hash_tag] -> hash_tag
          _ -> key
        end

      :crc_16_xmodem
      |> CRC.crc(hash_key)
      |> rem(range)
    end
  end
end
