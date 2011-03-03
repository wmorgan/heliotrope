# encoding: UTF-8

require 'whistlepig'
require 'oklahoma_mixer'
require 'set'

class Array
  def ordered_uniq
    s = Set.new
    select { |e| !s.member?(e) && s.add(e) }
  end

  def max_by
    inject([nil, nil]) do |(maxe, maxv), e|
      v = yield e
      if maxv.nil? || v > maxv
        [e, v]
      else
        [maxe, maxv]
      end
    end.first
  end
end

module Heliotrope
class Index
  QUERY_FILTER = Whistlepig::Query.new "", "-~deleted" # always filter out deleted messages

  ## these are things that can be set on a per-message basis. each one
  ## corresponds to a particular label, but labels are propagated at the thread
  ## level whereas state is not.
  MESSAGE_MUTABLE_STATE = Set.new %w(starred unread deleted)

  ## flags that are set per-message but are not modifiable by the user
  MESSAGE_IMMUTABLE_STATE = Set.new %w(attachment signed encrypted draft sent)

  MESSAGE_STATE = MESSAGE_MUTABLE_STATE + MESSAGE_IMMUTABLE_STATE

  SNIPPET_MAX_SIZE = 100 # chars

  def initialize base_dir
    #@store = Rufus::Tokyo::Cabinet.new File.join(base_dir, "pstore") # broken
    #@store = PStore.new File.join(base_dir, "pstore") # sucks
    @store = OklahomaMixer.open File.join(base_dir, "store.tch")
    @index = Whistlepig::Index.new File.join(base_dir, "index")
    @query = nil # we always have (at most) one active query
    @debug = false
    reset_timers!
  end

  def close
    @index.close
    @store.close
  end

  attr_reader :index_time, :store_time, :thread_time
  attr_accessor :debug

  def reset_timers!
    @index_time = @store_time = @thread_time = 0
  end

  def add_message message, state=[], labels=[], extra={}
    key = "docid/#{message.msgid}"
    if contains_key? key
      docid = load_int key
      threadid = load_int "thread/#{docid}"
      return [docid, threadid]
    end

    state = Set.new state
    state &= MESSAGE_MUTABLE_STATE # filter to the only states the user can set
    state << "attachment" if message.has_attachment? # set any immutable state
    state << "signed" if message.signed?
    state << "encrypted" if message.encrypted?

    ## add message to index
    docid = index! message

    ## add message to store
    messageinfo = write_messageinfo! message, state, docid, extra

    ## build thread structure, collecting any labels from threads that have
    ## been joined by adding this message.
    threadid, thread_structure, old_labels = thread_message! message

    ## get the thread snippet
    snippet = calc_thread_snippet thread_structure

    ## get the thread state
    thread_state = merge_thread_state thread_structure

    ## calculate the labels
    labels = Set.new(labels) - MESSAGE_STATE # you can't set these
    labels += thread_state # but i can
    #labels += merge_thread_labels(thread_structure) # you can have these, though
    labels += old_labels # you can have these, though

    ## write thread to store
    threadinfo = write_threadinfo! threadid, thread_structure, labels, thread_state, snippet

    ## add labels to every message in the thread (for search to work)
    write_thread_message_labels! thread_structure, labels

    ## congrats, you have a doc and a thread!
    [docid, threadid]
  end

  def update_message_state docid, state
    state = Set.new(state) & MESSAGE_MUTABLE_STATE

    ## update message state
    key = "state/#{docid}"
    old_mstate = load_set key
    new_mstate = (old_mstate - MESSAGE_MUTABLE_STATE) + state
    return nil if new_mstate == old_mstate
    write_set key, new_mstate

    ## load the threadinfo for this message
    threadid = load_int "threadid/#{docid}"
    threadinfo = load_hash "thread/#{threadid}"

    ## recalc thread snippet
    key = "tsnip/#{threadid}"
    old_snippet = load_string key
    new_snippet = calc_thread_snippet threadinfo[:structure]
    if old_snippet != new_snippet
      write_string key, new_snippet
    end

    ## recalc thread state and labels
    old_tstate = load_set "tstate/#{threadid}"
    new_tstate = merge_thread_state threadinfo[:structure]
    new_tlabels = nil

    if new_tstate != old_tstate
      write_set "tstate/#{threadid}", new_tstate

      ## update thread labels
      key = "tlabels/#{threadid}"
      old_tlabels = load_set key
      new_tlabels = (old_tlabels - MESSAGE_MUTABLE_STATE) + new_tstate
      write_set key, new_tlabels

      write_thread_message_labels! threadinfo[:structure], new_tlabels
    end

    [new_mstate, new_tstate, new_tlabels]
  end

  def update_thread_labels threadid, labels
    labels = Set.new(labels) - MESSAGE_STATE

    key = "tlabels/#{threadid}"
    old_tlabels = load_set key
    new_tlabels = (old_tlabels & MESSAGE_STATE) + labels
    write_set key, new_tlabels

    threadinfo = load_hash "thread/#{threadid}"
    write_thread_message_labels! threadinfo[:structure], new_tlabels

    new_tlabels
  end

  def contains_msgid? msgid; contains_key? "docid/#{msgid}" end

  def size; @index.size end

  def set_query query
    @index.teardown_query @query.whistlepig_q if @query # new query, drop old one
    @query = query.and QUERY_FILTER
    @index.setup_query @query.whistlepig_q
    @seen_threads = {}
  end

  def reset_query!
    @index.teardown_query @query.whistlepig_q
    @index.setup_query @query.whistlepig_q
    @seen_threads = {}
  end

  def get_some_results num
    return [] unless @query

    startt = Time.now
    threadids = []
    until threadids.size >= num
      docid = @index.run_query(@query.whistlepig_q, 1).first
      break unless docid
      threadid = load_int "threadid/#{docid}"
      raise "no threadid for doc #{docid}" unless threadid
      next if @seen_threads[threadid]
      @seen_threads[threadid] = true
      threadids << threadid
    end

    loadt = Time.now
    results = threadids.map { |id| load_threadinfo id }
    endt = Time.now
    #printf "# search %.1fms, load %.1fms\n", 1000 * (loadt - startt), 1000 * (endt - startt)
    results
  end

  def load_threadinfo threadid
    h = load_thread threadid
    h.merge! :thread_id => threadid,
      :state => load_set("tstate/#{threadid}"),
      :labels => load_set("tlabels/#{threadid}"),
      :snippet => load_string("tsnip/#{threadid}")
  end

  def load_messageinfo docid
    h = load_hash "doc/#{docid}"
    h.merge :state => load_set("state/#{docid}"),
      :thread_id => load_int("threadid/#{docid}"),
      :snippet => load_string("msnip/#{docid}"),
      :message_id => docid
  end

  def load_thread_messageinfos threadid
    h = load_thread(threadid) or return
    load_structured_messageinfo h[:structure]
  end

  def count_results
    startt = Time.now
    thread_ids = Set.new
    query = @query.clone
    @index.setup_query query.whistlepig_q
    begin
      while true
        docids = @index.run_query query.whistlepig_q, 1000
        docids.each do |docid|
          thread_id = load_int "threadid/#{docid}"
          thread_ids << thread_id
        end
        break if docids.size < 1000
      end
      elapsed = Time.now - startt
    ensure
      @index.teardown_query query.whistlepig_q
    end
    thread_ids.size
  end

private

  def calc_thread_snippet thread_structure
    docids = thread_structure.flatten.select { |id| id > 0 }
    first_unread = docids.find { |docid| load_set("state/#{docid}").member?("unread") }
    load_string("msnip/#{first_unread || docids.first}")
  end

  ## get the state for a thread by merging the state from each message
  def merge_thread_state thread_structure
    thread_structure.flatten.inject(Set.new) do |set, docid|
      set + (docid < 0 ? [] : load_set("state/#{docid}"))
    end
  end

  ## get the labels for a thread by merging the labels from each message
  def merge_thread_labels thread_structure
    thread_structure.flatten.inject(Set.new) do |set, docid|
      set + (docid < 0 ? [] : load_set("mlabels/#{docid}"))
    end
  end

  ## sync labels to all messages within the thread. necessary if you want
  ## search to work properly.
  def write_thread_message_labels! thread_structure, labels
    thread_structure.flatten.each do |docid|
      next if docid < 0 # psuedo-root
      key = "mlabels/#{docid}"
      oldlabels = load_set key
      write_set key, labels
      (oldlabels - labels).each { |l| puts "; removing ~#{l} from #{docid}" if @debug; @index.remove_label docid, l }
      (labels - oldlabels).each { |l| puts "; adding ~#{l} to #{docid}" if @debug; @index.add_label docid, l }
    end
  end

  def load_structured_messageinfo thread_structure, level=0
    id, *children = thread_structure
    doc = if id < 0
      {:type => "fake"}
    else
      load_messageinfo(id)
    end

    children.inject([[doc, level]]) { |a, c| a + load_structured_messageinfo(c, level + 1) }
  end

  def load_thread threadid; load_hash("thread/#{threadid}") end

  ## given a single message, which contains a (partial) path from it to an
  ## ancestor (which itself may or may not be the root), build up the thread
  ## structures. doesn't hit the search index, just the kv store.
  def thread_message! message
    startt = Time.now

    ## build the path of msgids from leaf to ancestor
    ids = [message.msgid] + message.refs.reverse
    seen = {}
    ids = ids.map { |x| seen[x] = true && x unless seen[x] }.compact

    ## write parent/child relationships
    if ids.size > 1
      ids[0 .. -2].zip(ids[1 .. -1]).each do |id, parent|
        pkey = "pmsgid/#{id}"
        next if contains_key? pkey # don't overwrite--potential for mischief?
        write_string pkey, parent

        ckey = "cmsgids/#{parent}"
        v = load_set(ckey)
        v << id
        write_set ckey, v
      end
    end

    ## find the root of the whole thread
    root = ids.first
    seen = {} # guard against loops
    while(id = load_string("pmsgid/#{root}"))
      #puts "parent of #{root} is #{id}"
      break if seen[id]; seen[id] = true
      root = id
    end

    ## get the thread structure in terms of docids docs we've actually seen.
    ## generate psuedo-docids to join trees with parents we haven't seen yet
    ## when necessary.
    thread_structure = build_thread_structure_from root
    #puts "thread structure is #{thread_structure.inspect}"
    threadid = thread_structure.first # might actually be a psuedo-docid
    #puts "root msgid is #{root.inspect}, root docid is #{threadid}"

    ## if any of these docs are roots of old threads, delete those old threads,
    ## but keep track of all the labels we've seen
    old_labels = thread_structure.flatten.inject(Set.new) do |labels, id|
      tkey = "thread/#{id}"
      labels + if contains_key? tkey
        lkey = "tlabels/#{id}"
        v = load_set lkey
        @store.delete lkey
        @store.delete tkey
        v
      else
        Set.new
      end
    end

    ## write the thread ids for all documents. we need this at search time to
    ## do the message->thread mapping.
    thread_structure.flatten.each do |id|
      next if id < 0 # pseudo root
      write_int "threadid/#{id}", threadid
    end

    @thread_time += (Time.now - startt)
    [threadid, thread_structure, old_labels]
  end

  ## builds an array representation of the thread, filling in only those
  ## messages that we actually have in the store, and making psuedo-message
  ## roots for the cases when we have seen multiple children but not the
  ## parent.
  def build_thread_structure_from msgid, seen={}
    return [] if seen[msgid]

    docid = load_int "docid/#{msgid}"
    children = load_set "cmsgids/#{msgid}"
    #puts "> children of #{msgid} are #{children.inspect}"

    seen[msgid] = true
    child_thread_structures = children.map { |c| build_thread_structure_from(c, seen) }.compact

    #puts "< bts(#{msgid}): docid=#{docid.inspect}, child_structs=#{child_thread_structures.inspect}"
    if docid
      if child_thread_structures.empty?
        [docid.to_i]
      else
        [docid.to_i] + child_thread_structures
      end
    else
      case child_thread_structures.size
      when 0; nil
      when 1; child_thread_structures.first
      else # need to make a psuedo root
        psuedo_root = -child_thread_structures.first.first # weird?
        [psuedo_root] + child_thread_structures
      end
    end
  end

  def write_threadinfo! threadid, thread_structure, labels, state, snippet
    subject = date = from = to = has_attachment = nil

    docids = thread_structure.flatten.select { |x| x > 0 }
    messages = docids.map { |id| load_hash("doc/#{id}") }

    participants = messages.map { |m| m[:from] }.ordered_uniq
    first_message = messages.first # just take the root
    last_message = messages.max_by { |m| m[:date] }

    threadinfo = {
      :subject => first_message[:subject],
      :date => last_message[:date],
      :participants => participants,
      :size => docids.size,
      :structure => thread_structure,
    }

    write_hash "thread/#{threadid}", threadinfo
    write_set "tlabels/#{threadid}", labels
    write_set "tstate/#{threadid}", state
    write_string "tsnip/#{threadid}", snippet
    threadinfo
  end

  def index! message
    ## make the entry
    startt = Time.now
    entry = Whistlepig::Entry.new
    entry.add_string "msgid", message.msgid
    entry.add_string "from", message.from.indexable_text.downcase
    entry.add_string "to", message.recipients.map { |x| x.indexable_text }.join(" ").downcase
    entry.add_string "subject", message.subject.downcase
    entry.add_string "date", message.date.to_s
    entry.add_string "body", message.indexable_text.downcase
    @index_time += Time.now - startt

    @index.add_entry entry
  end

  def write_messageinfo! message, state, docid, extra
    ## write it to the store
    startt = Time.now
    messageinfo = {
      :subject => message.subject,
      :date => message.date,
      :from => message.from.to_s,
      :to => message.recipients.map { |x| x.to_s },
      :has_attachment => message.has_attachment?,
    }.merge extra

    ## add it to the store
    write_hash "doc/#{docid}", messageinfo
    write_set "state/#{docid}", state
    write_int "docid/#{message.msgid}", docid
    write_string "msnip/#{docid}", message.snippet[0, SNIPPET_MAX_SIZE]
    @store_time += Time.now - startt

    messageinfo
  end

  def in_ruby19_hell?
    @in_ruby19_hell = "".respond_to?(:encoding) if @in_ruby19_hell.nil?
    @in_ruby19_hell
  end

  ## so horrible. strings marshalled in ruby < 1.9 come back as binary in ruby
  ## 1.9. so either we break index compatibility when crossing ruby versions,
  ## OR we have to MANUALLY tell Ruby that every string that comes back from
  ## the store is in utf8.
  ##
  ## we take the second approach, but it burnssss usssss.
  STORE_ENCODING = Encoding::UTF_8 if defined? Encoding

  def munge o
    return o unless in_ruby19_hell?
    case o
    when String; o.dup.force_encoding STORE_ENCODING
    when Hash; o.each { |k, v| o[k] = v.dup.force_encoding(STORE_ENCODING) if v.is_a?(String) }
    when Set; Set.new(o.map { |e| e.dup.force_encoding(STORE_ENCODING) })
    else; o
    end
  end

  def load_string key; munge(@store[key]) end
  def write_string key, value
    puts "; #{key} => #{value.inspect}" if @debug
    @store[key] = value.to_s
  end

  def load_array key; @store.member?(key) ? munge(Marshal.load(@store[key])) : [] end
  def write_array key, value
    puts "; #{key} => #{value.inspect}" if @debug
    @store[key] = Marshal.dump(value.to_a)
  end

  def load_hash key; @store.member?(key) ? munge(Marshal.load(@store[key])) : {} end
  def write_hash key, value
    puts "; #{key} => #{value.inspect}" if @debug
    @store[key] = Marshal.dump(value.to_hash)
  end

  def load_int key; @store.member?(key) ? Marshal.load(@store[key]) : nil end
  def write_int key, value
    puts "; #{key} => #{value.inspect}" if @debug
    @store[key] = Marshal.dump(value.to_i)
  end

  def load_set key; @store.member?(key) ? munge(Set.new(Marshal.load(@store[key]))) : Set.new end
  def write_set key, value
    puts "; #{key} => #{value.inspect}" if @debug
    @store[key] = Marshal.dump(value.to_set.to_a)
  end

  def contains_key? key; @store.member? key end
end
end
