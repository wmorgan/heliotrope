require 'rubygems'
require 'fastthread'
require 'heliotrope'

module Heliotrope

## Transactions, atomicity, rollbacks, etc.:
##
## Calls to add_document are atomic, and lock the entire server. In other
## words, while the call is occurring, all searches are suspended, and should
## the call fail (and an exception be delivered to the client), none of the
## documents are added to the system.
class Server
  def initialize bucket, index, quarker, port
    @bucket = bucket
    @index = index
    @port = port
    @quarker = quarker
    @server = nil
  end

  ## do whatever thrift stuff is necessary to actually start up the
  ## connection.
  ##
  ## TODO: allow configuration of thrift protocol and transport.
  def start
    return if @server
    processor = HeliotropeService::Processor.new self
    transport = Thrift::ServerSocket.new @port
    transportFactory = Thrift::BufferedTransportFactory.new # untranslated C++ idiom :(
    @server = Thrift::ThreadedServer.new processor, transport, transportFactory
  end

  def add_documents docs
    threads = @threader.thread docs, @bucket
    docids = add_to_index docs, threads
    add_to_bucket docs, threads, docids
  end

private

  def add_to_bucket objects
    @bucket_mutex.synchronize do
      next_docid = @bucket.get("next-docid") || 0
      objects.delete_if { |o| @bucket.has_key? o.key }
      objects.each do |o|
        o.docid = next_docid
        next_docid += 1
        raise Error, "no id on object" unless o.id?
        @bucket.put o.key, o
        t = find_and_update_thread_for o
        @bucket.put t.key, t
        @bucket.put "thread-key-for-docid-#{o.docid}", t.key
      end
      @bucket.put "next-docid", next_docid
    end
  end

  def index objects
    objects = [objects].flatten
    @index_mutex.synchronize { @index.add_to_index objects }
  end

  def find_and_update_thread_for o
  end
end

end
