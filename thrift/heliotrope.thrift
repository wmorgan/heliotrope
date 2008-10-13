/**
 * The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 */

namespace rb Heliotrope

struct Document {
  1: i32 id,
  2: map<string, string> metadata,
  3: string body, /* not necessarily filled in */
  4: i32 body_size,
}

struct Tree {
  1: i32 id,
  2: string type,
  3: map<string, string> metadata,
  4: list<i32> doc_ids,
  5: list<i32> doc_depths,
}

struct SearchResult {
  1: i32 tree_id,
  2: i32 doc_id,
  3: i32 search_position, /* not necessarily filled in */
}

struct AddResult {
  1: i32 tree_id,
  2: i32 doc_id,
}

exception GeneralError {
  1: string message
}

exception NoSuchIdError {
  1: string message
}

exception NotAChildError {
  1: string message
}

service HeliotropeService {
  list<SearchResult> search(1:string tree_type, 2:string query, 3:i32 start, 4:i32 offset, 5:map<string, string> params) throws (1:GeneralError e),
  i32 search_size(1:string tree_type, 2:string query, 3:map<string, string> params) throws (1:GeneralError e),

  Document get_document(1:i32 doc_id) throws (1:GeneralError e, 2:NoSuchIdError nsi), 
  Tree get_tree(1:i32 doc_id) throws (1:GeneralError e, 2:NoSuchIdError nsi), 
  list<Tree> get_trees_rooted_at_doc(1:i32 doc_id, 2:string tree_type) throws (1:GeneralError e, 2:NoSuchIdError nsi),

  string get_document_body(1:i32 doc_id, 2:i32 start, 3:i32 offset) throws (1:GeneralError e, 2:NoSuchIdError nsi),
  string update_document_metadata(1:i32 doc_id, 2:map<string, string> updates) throws (1:GeneralError e, 2:NoSuchIdError nsi),

  list<AddResult> add_documents(1:list<Document> o) throws (1:GeneralError e),

  void attach_as_child(1:string tree_type, 2:i32 parent_doc_id, 3:i32 child_doc_id) throws (1:GeneralError e, 2:NoSuchIdError nsi),
  void detach_as_child(1:string tree_type, 2:i32 parent_doc_id, 3:i32 child_doc_id) throws (1:GeneralError e, 2:NoSuchIdError nsi, 3:NotAChildError nac),
}
