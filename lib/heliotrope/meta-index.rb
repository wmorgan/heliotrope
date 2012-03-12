# encoding: UTF-8

require 'whistlepig'
require 'leveldb'
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
class MetaIndex
  class VersionMismatchError < StandardError
    attr_reader :have_version, :want_version

    def initialize have_version, want_version
      @have_version = have_version
      @want_version = want_version
    end

    def message
      "index is version #{have_version.inspect} but I am expecting #{want_version.inspect}"
    end
  end

  ## these are things that can be set on a per-message basis. each one
  ## corresponds to a particular label, but labels are propagated at the
  ## thread level whereas state is not.
  MESSAGE_MUTABLE_STATE = Set.new %w(starred unread deleted)
  ## flags that are set per-message but are not modifiable by the user
  MESSAGE_IMMUTABLE_STATE = Set.new %w(attachment signed encrypted draft sent)
  MESSAGE_STATE = MESSAGE_MUTABLE_STATE + MESSAGE_IMMUTABLE_STATE
  ## if you change any of those state things, be sure to update
  ## heliotrope-client as well.

  SNIPPET_MAX_SIZE = 100 # chars

  def initialize store, index, hooks, opts={}
    @store = store
    @index = index
    @hooks = hooks
    @query = nil # we always have (at most) one active query
    @debug = false
    reset_timers!
    check_version! if @index
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

  def version; [major_version, minor_version].join(".") end
  def major_version; 0 end
  def minor_version; 1 end

  ## helper factory that assumes console access
  def self.load_or_die! store, index, hooks
    begin
      Heliotrope::MetaIndex.new store, index, hooks
    rescue Heliotrope::MetaIndex::VersionMismatchError => e
      $stderr.puts "Version mismatch error: #{e.message}."
      $stderr.puts "Try running #{File.dirname $0}/heliotrope-upgrade-index."
      abort
    end
  end

  def check_version! # throws a VersionMismatchError
    my_version = [major_version, minor_version].join(".")

    if @index.size == 0
      write_string "version", my_version
    else
      disk_version = load_string "version"
      raise VersionMismatchError.new(disk_version, my_version) unless my_version == disk_version
    end
  end

  def add_message message, state=[], labels=[], extra={}
    key = "docid/#{message.safe_msgid}"
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
    index_docid = index! message
    docid = gen_new_docid!

    ## write index_docid <-> docid mapping
    write_docid_mapping! docid, index_docid

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

    ## add the labels to the set of all labels we've ever seen
    add_labels_to_labellist! labels

    ## congrats, you have a doc and a thread!
    [docid, threadid]
  end

  ## add or update a contact
  def touch_contact! contact, timestamp=Time.now.to_i
    old_record = load_hash "c/#{contact.email.downcase}"
    if (old_record[:timestamp] || 0) < timestamp
      record = { :name => contact.name, :email => contact.email, :timestamp => timestamp }
      write_hash "c/#{contact.email.downcase}", record
      write_hash "c/#{contact.name.downcase}", record if contact.name
      old_record[:timestamp].nil? # return true if it's a brand-new record
    end
  end

  def contacts opts={}
    num = opts[:num] || 20
    prefix = opts[:prefix]

    iter = if prefix
      prefix = prefix.downcase.gsub("/", "") # oh yeah
      @store.each(:from => "c/#{prefix}", :to => "c/#{prefix}~") # ~ is the largest character ha ha ha :( :( :(
    else
      @store.each(:from => "c/")
    end

    iter.take(num).map { |k, v| load_hash k }
  end

  ## returns the new message state
  def update_message_state docid, state
    state = Set.new(state) & MESSAGE_MUTABLE_STATE

    changed, new_state = really_update_message_state docid, state
    if changed
      threadid = load_int "threadid/#{docid}"
      threadinfo = load_hash "thread/#{threadid}"
      rebuild_all_thread_metadata threadid, threadinfo
    end

    new_state
  end

  def update_thread_state threadid, state
    state = Set.new(state) & MESSAGE_MUTABLE_STATE

    threadinfo = load_hash "thread/#{threadid}"
    docids = threadinfo[:structure].flatten.select { |id| id > 0 }

    changed = false
    docids.each do |docid|
      this_changed, _ = really_update_message_state docid, state
      changed ||= this_changed
    end

    if changed
      threadinfo = rebuild_all_thread_metadata threadid, threadinfo
    else
      load_set "tstate/#{threadid}"
    end
  end

  def update_thread_labels threadid, labels
    labels = Set.new(labels) - MESSAGE_STATE

    ## add the labels to the set of all labels we've ever seen. do this
    ## first because it also does some validation.
    add_labels_to_labellist! labels

    key = "tlabels/#{threadid}"
    old_tlabels = load_set key
    new_tlabels = (old_tlabels & MESSAGE_STATE) + labels
    write_set key, new_tlabels

    threadinfo = load_hash "thread/#{threadid}"
    write_thread_message_labels! threadinfo[:structure], new_tlabels

    new_tlabels
  end

  def contains_safe_msgid? safe_msgid; contains_key? "docid/#{safe_msgid}" end

  def fetch_docid_for_safe_msgid safe_msgid
    key = "docid/#{safe_msgid}"
    if contains_key? key
      docid = load_int key
      threadid = load_int "threadid/#{docid}"
      [docid, threadid]
    end
  end

  def size; @index.size end

  def set_query query
    @index.teardown_query @query.whistlepig_q if @query # new query, drop old one
    @query = query
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
      index_docid = @index.run_query(@query.whistlepig_q, 1).first
      break unless index_docid
      doc_id, thread_id = get_thread_id_from_index_docid index_docid
      next if @seen_threads[thread_id]
      @seen_threads[thread_id] = true
      threadids << thread_id
    end

    loadt = Time.now
    results = threadids.map { |id| load_threadinfo id }
    endt = Time.now
    #printf "# search %.1fms, load %.1fms\n", 1000 * (loadt - startt), 1000 * (endt - startt)
    results
  end

  def load_threadinfo threadid
    h = load_thread(threadid) or return
    h.merge! :thread_id => threadid,
      :state => load_set("tstate/#{threadid}"),
      :labels => load_set("tlabels/#{threadid}"),
      :snippet => load_string("tsnip/#{threadid}"),
      :unread_participants => load_set("turps/#{threadid}")
  end

  def load_messageinfo docid
    key = "doc/#{docid}"
    return unless contains_key? key
    h = load_hash key
    h.merge :state => load_set("state/#{docid}"),
      :labels => load_set("mlabels/#{docid}"),
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
        docids.each do |index_docid|
          doc_id, thread_id = get_thread_id_from_index_docid index_docid
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

  def all_labels
    load_set "labellist"
  end

  ## expensive! runs a query for each label and sees if there are any docs for
  ## it
  def prune_labels!
    pruned_labels = all_labels.reject do |l|
      query = Whistlepig::Query.new "body", "~#{l}"
      @index.setup_query query
      docids = begin
        @index.run_query query, 1
      ensure
        @index.teardown_query query
      end

      docids.empty?
    end

    write_set "labellist", pruned_labels
  end

  def indexable_text_for thing
    orig = thing.indexable_text
    transformed = @hooks.run "transform-text", :text => orig
    transformed = Decoder.encode_as_utf8 transformed
    transformed || orig
  end

  def write_docid_mapping! store_docid, index_docid
    write_int "i2s/#{index_docid}", store_docid # redirect index to store
    write_int "s2i/#{store_docid}", index_docid # reidrect store to index
  end

private

  def get_thread_id_from_index_docid index_docid
    store_docid = load_int("i2s/#{index_docid}")
    thread_id = load_int "threadid/#{store_docid}"
    raise "no thread_id for doc #{store_docid.inspect} (index doc #{index_docid.inspect})" unless thread_id # your index is corrupt!
    [store_docid, thread_id]
  end

  def get_index_docid_from_store_docid store_docid
    load_int "s2i/#{store_docid}"
  end


  def gen_new_docid!
    v = load_int("next_docid") || 1
    write_int "next_docid", v + 1
    v
  end

  def is_valid_whistlepig_token? l
    # copy logic from whistlepig's query-parser.lex
    l =~ /^[^\(\)"\-~:\*][^\(\)":]*$/
  end

  def really_update_message_state docid, state
    ## update message state
    key = "state/#{docid}"
    old_mstate = load_set key
    new_mstate = (old_mstate - MESSAGE_MUTABLE_STATE) + state

    changed = new_mstate != old_mstate
    write_set key, new_mstate if changed
    [changed, new_mstate]
  end

  ## rebuild snippet, labels, read/unread participants, etc.  for a
  ## thread. useful if something about one of the thread's messages has
  ## changed.
  ##
  ## returns the new thread state
  def rebuild_all_thread_metadata threadid, threadinfo
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

    ## recalc the unread participants
    docids = threadinfo[:structure].flatten.select { |x| x > 0 }
    messages = docids.map { |id| load_hash("doc/#{id}") }
    states = docids.map { |id| load_hash("state/#{id}") }

    write_unread_participants! threadid, messages, states

    new_tstate
  end

  def write_unread_participants! threadid, messages, states
    unread_participants = messages.zip(states).map do |m, state|
      m[:from] if state.member?("unread")
    end.compact.to_set
    write_set "turps/#{threadid}", unread_participants
  end

  class InvalidLabelError < StandardError
    def initialize label
      super "#{label} is an invalid label"
    end
  end

  def add_labels_to_labellist! labels
    labels.each { |l| raise InvalidLabelError, l unless is_valid_whistlepig_token?(l) }
    key = "labellist"
    labellist = load_set key
    labellist_new = labellist + labels.select { |l| is_valid_whistlepig_token? l }
    write_set key, labellist_new unless labellist == labellist_new
  end

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

      ## write to index
      index_docid = get_index_docid_from_store_docid docid
      (oldlabels - labels).each do |l|
        puts "; removing ~#{l} from #{index_docid} (store #{docid})" if @debug
        @index.remove_label index_docid, l
      end
      (labels - oldlabels).each do |l|
        puts "; adding ~#{l} to #{index_docid} (store #{docid})" if @debug
        @index.add_label index_docid, l
      end
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

  def load_thread threadid
    key = "thread/#{threadid}"
    return unless contains_key? key
    load_hash key
  end

  ## given a single message, which contains a (partial) path from it to an
  ## ancestor (which itself may or may not be the root), build up the thread
  ## structures. doesn't hit the search index, just the kv store.
  def thread_message! message
    startt = Time.now

    ## build the path of msgids from leaf to ancestor
    ids = [message.safe_msgid] + message.safe_refs.reverse
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
  def build_thread_structure_from safe_msgid, seen={}
    return nil if seen[safe_msgid]

    docid = load_int "docid/#{safe_msgid}"
    children = load_set "cmsgids/#{safe_msgid}"
    #puts "> children of #{msgid} are #{children.inspect}"

    seen[safe_msgid] = true
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
    states = docids.map { |id| load_hash("state/#{id}") }

    participants = messages.map { |m| m[:from] }.ordered_uniq
    direct_recipients = messages.map { |m| m[:to] }.flatten.to_set
    indirect_recipients = messages.map { |m| m[:cc] }.flatten.to_set

    first_message = messages.first # just take the root
    last_message = messages.max_by { |m| m[:date] }

    threadinfo = {
      :subject => first_message[:subject],
      :date => last_message[:date],
      :participants => participants,
      :direct_recipients => direct_recipients,
      :indirect_recipients => indirect_recipients,
      :size => docids.size,
      :structure => thread_structure,
    }

    write_hash "thread/#{threadid}", threadinfo
    write_set "tlabels/#{threadid}", labels
    write_set "tstate/#{threadid}", state
    write_string "tsnip/#{threadid}", snippet

    write_unread_participants! threadid, messages, states

    threadinfo
  end

  def index! message
    ## make the entry
    startt = Time.now
    entry = Whistlepig::Entry.new
    entry.add_string "from", indexable_text_for(message.from).downcase
    entry.add_string "to", message.recipients.map { |x| indexable_text_for x }.join(" ").downcase
    entry.add_string "subject", message.subject.downcase
    entry.add_string "date", message.date.to_s
    entry.add_string "body", indexable_text_for(message).downcase
    @index_time += Time.now - startt

    @index.add_entry entry
  end

  def write_messageinfo! message, state, docid, extra
    ## write it to the store
    startt = Time.now
    messageinfo = {
      :subject => message.subject,
      :date => message.date,
      :from => message.from.to_email_address,
      :to => message.direct_recipients.map { |x| x.to_email_address },
      :cc => message.indirect_recipients.map { |x| x.to_email_address },
      :has_attachment => message.has_attachment?,
    }.merge extra

    ## add it to the store
    write_hash "doc/#{docid}", messageinfo
    write_set "state/#{docid}", state
    write_int "docid/#{message.safe_msgid}", docid
    write_string "msnip/#{docid}", message.snippet[0, SNIPPET_MAX_SIZE]
    @store_time += Time.now - startt

    messageinfo
  end

  ## storing stuff is tricky
  ##
  ## strings can be stored directly but they MUST be marked (via
  ## #force_encoding) as binary, otherwise OklahomerMixer will truncate (!!!)
  ## #them if they contain any super-ASCII characters. (we could marshal
  ## #strings, but it costs quite a few bytes.)
  ##
  ## other objects are just marshalled, which is fine, and in ruby 1.9, string
  ## encodings will be preserved. HOWEVER, we need to recursively find all
  ## strings and mark them as utf-8 anyways, since they might've been
  ## marshalled by a 1.8 process, in which case they will come back as binary.
  ##
  ## once the entire world is safely in 1.9 and we never have a chance of
  ## someone first using 1.8, then switching to 1.9, we can remove some of this
  ## sillyness.

  STORE_ENCODING = Encoding::UTF_8 if Decoder.in_ruby19_hell?

  def munge o
    return o unless Decoder.in_ruby19_hell?
    case o
    when String; o.dup.force_encoding STORE_ENCODING
    when Hash; o.each { |k, v| o[k] = munge(v) }
    when Set; Set.new(o.map { |e| munge(e) })
    when Array; o.map { |e| munge(e) }
    else; o
    end
  end

  def protect_string s
    if Decoder.in_ruby19_hell?
      s.force_encoding "binary"
    else
      s
    end
  end

  def load_string key; munge(@store[key]) end
  def write_string key, value
    puts "; #{key} => #{value.inspect}" if @debug
    @store[key] = protect_string(value.to_s)
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
