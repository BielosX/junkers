#lang racket

(require racket/cmdline
         fuse
         kw-utils/partial
         racket/async-channel
         net/http-client
         json
         racket/match
         racket/port
         data/maybe)

(struct folder (name last-modified folder-id))
(struct file (name last-modified entry-id))
(struct http-response (status headers body))
(struct http-request (get))
(struct node (node-id-to-folder-id folder-id-to-node-id next-node-id))

(define (list-find-first lst predicate)
  (match lst
         [(list-rest head tail) (if (predicate head)
                                  (just head)
                                  (list-find-first tail predicate))]
         [empty nothing]))

(define (list-drop lst cnt)
  (if (zero? cnt)
    lst
    (match lst
                [(list-rest head tail) (list-drop tail (sub1 cnt))]
                [empty empty])))

(define (safe-hash-ref hash key)
  (define result (hash-ref hash key nothing))
  (if (nothing? result)
    result
    (just result)))

(define (create-http-request host token)
  (http-request (partial http-get host #:token token)))

(define (folder-from-htable htable)
  (folder (hash-ref htable 'name)
          (hash-ref htable 'lastModified)
          (hash-ref htable 'folder_id)))

(define (file-from-htable htable)
  (file (hash-ref htable 'name)
        (hash-ref htable 'last_modified)
        (hash-ref htable 'entry_id)))

(define (parse-folder-content json-content)
  (define (to-content-list folders files)
    (match (cons folders files)
           [(cons (list-rest head tail) _) (cons (folder-from-htable head) (to-content-list tail files))]
           [(cons empty (list-rest head tail)) (cons (file-from-htable head) (to-content-list empty tail))]
           [(cons empty empty) empty]))
  (define content (string->jsexpr json-content))
  (to-content-list (hash-ref content 'folders empty) (hash-ref content 'files empty)))

(define (append-auth headers token)
    (cons (string-append "Authorization: Bearer " token) headers))

(define (http-sendrecv-secure host uri #:method method #:headers headers)
  (http-sendrecv host uri #:method method #:headers headers #:ssl? #t))

(define (http-get host uri #:headers [headers empty] #:token token)
  (define (status-number line) (string->number (list-ref (string-split (bytes->string/utf-8 line)) 1)))
  (match-define-values (status headers-list port) (http-sendrecv-secure host uri #:method "GET" #:headers (append-auth headers token)))
  (define response (http-response (status-number status) headers-list (port->string port)))
  (begin
    (close-input-port port)
    response))

(define (get-root-id http-request #:headers [headers empty])
  (define with-accept (cons "Accept: application/json" headers))
  (hash-ref (string->jsexpr (http-response-body ((http-request-get http-request) "/pubapi/v1/fs/" #:headers with-accept))) 'folder_id))

(define (list-folder http-request folder-id #:headers [headers empty])
  (define with-accept (cons "Accept: application/json" headers))
  (define response ((http-request-get http-request) (string-append "/pubapi/v1/fs/ids/folder/" folder-id) #:headers with-accept))
  (define (extracting-entry-name entry)
    (match entry
           [(file name _ _) name]
           [(folder name _ _) name]))
  (if (equal? (http-response-status response) 200)
    (sort
      (parse-folder-content (http-response-body response))
      string<=?
      #:key extracting-entry-name)
    empty))


(define (mkdir #:nodeid nodeid #:name name #:mode mode #:umask umask #:reply reply-entry #:error error)
  (begin
    (displayln name)
    (reply-entry #:generation 0 #:entry-valid  (timespec 1 0) #:attr-valid  (timespec 1 0)
                 #:inode 3 #:rdev 0 #:size 13 #:blocks 1
                 #:atime  (timespec 1381237736 0) #:mtime  (timespec 1381237736 0)
                 #:ctime  (timespec 1381237736 0) #:kind 'S_IFREG
                 #:perm '(S_IRUSR S_IWUSR S_IRGRP S_IROTH)
                 #:nlink 1 #:uid 1000 #:gid 1000)
    )
  )

(define (by-name entry-name)
  (lambda (entry) (match entry
                         [(file name _ _) (string=? entry-name name)]
                         [(folder name _ _) (string=? entry-name name)])))

(define (handler-lookup http-request #:channel channel #:nodeid nodeid #:name name #:reply reply-entry #:error error)
  (define node-id-cache (async-channel-get channel))
  (define folder-id (hash-ref (node-node-id-to-folder-id node-id-cache) nodeid))
  (define entry (list-find-first (list-folder http-request folder-id) (by-name (path->string name))))
  (define nid-to-fid (node-node-id-to-folder-id node-id-cache))
  (define fid-to-nid (node-folder-id-to-node-id node-id-cache))
  (define next-id (node-next-node-id node-id-cache))
  (define (new-cache-and-id entry-id)
    (if (hash-has-key? fid-to-nid entry-id)
      (cons (hash-ref fid-to-nid entry-id) node-id-cache)
      (cons next-id (node (hash-set nid-to-fid next-id entry-id) (hash-set fid-to-nid entry-id next-id) (add1 next-id)))
      ))
  (define (entry-node-id entry)
    (match entry
           [(folder _ _ folder-id) (new-cache-and-id folder-id)]
           [(file _ _ entry-id) (new-cache-and-id entry-id)]))
  (define (entry-kind entry)
    (match entry
           [(folder _ _ _) 'S_IFDIR]
           [(file _ _ _) 'S_IFREG]))
  (begin
    (displayln (string-append "inode: " (~a nodeid)))
    (displayln (string-append "lookup name: " (path->string name)))
    (match entry
           [(nothing) (begin
                        (displayln "No matching entry")
                        (async-channel-put channel node-id-cache)
                        (error 'ENOENT))]
           [(just e) (begin
                       (define cache-and-id (entry-node-id e))
                       (displayln "Found matching entry")
                       (reply-entry #:generation 0
                                    #:entry-valid  (timespec 1 0) #:attr-valid  (timespec 1 0)
                                    #:inode (car cache-and-id) #:rdev 0 #:size 13 #:blocks 1
                                    #:atime  (timespec 1381237736 0) #:mtime  (timespec 1381237736 0)
                                    #:ctime  (timespec 1381237736 0) #:kind (entry-kind e)
                                    #:perm '(S_IRUSR S_IWUSR S_IRGRP S_IROTH)
                                    #:nlink 1 #:uid 1000 #:gid 1000)
                       (async-channel-put channel (cdr cache-and-id))
                       )])))

(define (handle-init http-request #:channel channel)
  (define root-id (get-root-id http-request))
  (begin
    (displayln "init")
    (displayln (string-append "root_id: " root-id))
    (async-channel-put channel (node (hash 1 root-id) (hash root-id 1) 2))
    #t))

(define (content-to-dentry content
                           parent-nodeid
                           offset
                           reply-add
                           reply-done)
  (define (add-file name inode offset) (reply-add #:inode inode #:offset offset #:kind 'S_IFREG #:name (string->path name)))
  (define (add-folder name inode offset) (reply-add #:inode inode #:offset offset #:kind 'S_IFDIR #:name (string->path name)))
  (match content
         [(list-rest head tail) (match head
                                       [(file name _ entry-id) (begin
                                                                   (displayln (string-append "filename: " name))
                                                                   (add-file name parent-nodeid  offset)
                                                                   (content-to-dentry tail
                                                                                      parent-nodeid
                                                                                      (add1 offset)
                                                                                      reply-add
                                                                                      reply-done))]
                                       [(folder name _ folder-id) (begin
                                                                   (displayln (string-append "folder name " name))
                                                                      (add-folder name parent-nodeid offset)
                                                                      (content-to-dentry tail
                                                                                         parent-nodeid
                                                                                         (add1 offset)
                                                                                         reply-add
                                                                                         reply-done))])]
         [empty (begin
                  (displayln "Empty list")
                  (reply-done)
                  )]))

(define (handler-readdir http-request
                         #:channel channel
                         #:nodeid nodeid
                         #:info info
                         #:offset offset
                         #:add reply-add
                         #:reply reply-done
                         #:error error)
  (define node-id-cache (async-channel-get channel))
  (define fid-to-nid (node-folder-id-to-node-id node-id-cache))
  (define nid-to-fid (node-node-id-to-folder-id node-id-cache))
  (define next-id (node-next-node-id node-id-cache))
  (define current-folder-id (safe-hash-ref nid-to-fid nodeid))
  (begin
    (displayln (string-append "readdir nodeid: " (~a nodeid)))
    (match current-folder-id
           [(nothing) (begin
                        (displayln (string-append "no entry for nodeid: " (~a nodeid)))
                        (async-channel-put channel node-id-cache)
                        (error 'ENOENT))]
           [(just id) (begin
                        (displayln (string-append "offset: " (~a offset)))
                        (displayln (string-append "found entry for nodeid: " (~a id)))
                        (handle-list-folder http-request id nodeid offset reply-add reply-done error)
                        (async-channel-put channel node-id-cache))])))

(define (handle-list-folder request id node-id offset reply-add reply-done error)
  (define result (list-drop (list-folder request id) offset))
  (if (empty? result)
    (error 'ENOENT)
    (content-to-dentry result node-id 1 reply-add reply-done)))


(define (getattr #:nodeid nodeid #:info info #:reply reply-attr #:error error)
  (begin
      (displayln (string-append "trying to get attr for inode: " (~a nodeid)))
      (match nodeid
             [1 (reply-attr #:attr-valid  (timespec 1 0)
                             #:inode 1 #:rdev 0 #:size 0 #:blocks 0
                             #:atime  (timespec 1381237736 0) #:mtime  (timespec 1381237736 0)
                             #:ctime  (timespec 1381237736 0) #:kind 'S_IFDIR
                             #:perm '(S_IRUSR S_IWUSR S_IXUSR S_IRGRP S_IXGRP S_IROTH S_IXOTH)
                             #:nlink 1 #:uid 1000 #:gid 1000)]
             [_ (begin
                    (displayln (string-append "getattr inode: " (~a nodeid)))
                    (error 'ENOENT)
                  ) ])
        )
  )

(define (fs http-request)
  (let ([work-channel (make-async-channel #f)])
      (make-filesystem #:lookup (partial handler-lookup http-request #:channel work-channel)
                       #:init (partial handle-init http-request #:channel work-channel)
                       #:readdir (partial handler-readdir http-request #:channel work-channel)
                       #:getattr getattr
                       #:mkdir mkdir)))

(struct args ([domain #:mutable] [token #:mutable] [path #:mutable]))

(define program-args (args "" "" ""))

(define (request-from-args args) (create-http-request (args-domain program-args) (args-token program-args)))

(module+ main #f
        (begin
          (command-line
            #:once-each
            [("-d" "--domain") dmn "Egnyte domain" (set-args-domain! program-args dmn)]
            [("-t" "--token") tkn "Token" (set-args-token! program-args tkn)]
            [("-p" "--path") pth "Path to root" (set-args-path! program-args pth)])
          (mount-filesystem (fs (request-from-args program-args)) (string->path (args-path program-args)) '("default_permissions" "large_read" "direct_io" "hard_remove"))))
