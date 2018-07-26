#lang racket

(require racket/cmdline)
(require fuse)
(require kw-utils/partial)
(require racket/async-channel)
(require net/http-client)
(require json)
(require racket/match)
(require racket/port)
(require data/maybe)

(struct folder (name last-modified folder-id))
(struct file (name last-modified entry-id))
(struct http-response (status headers body))
(struct http-request (get))
(struct node (node-id-to-folder-id folder-id-to-node-id next-node-id))

(define (safe-hash-ref hash key)
  (define result (hash-ref hash key nothing))
  (if (nothing? result)
    result
    (just result)))

(define (comparing-by-name fst snd)
  (match* (fst snd)
         [((folder name1 _ _) (folder name2 _ _)) (string<=? name1 name2)]
         [((folder name1 _ _) (file name2 _ _)) (string<=? name1 name2)]
         [((file name1 _ _) (folder name2 _ _)) (string<=? name1 name2)]
         [((file name1 _ _) (file name2 _ _)) (string<=? name1 name2)]))

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
  (sort
    (parse-folder-content (http-response-body ((http-request-get http-request) (string-append "/pubapi/v1/fs/ids/folder/" folder-id) #:headers with-accept)))
    comparing-by-name))


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

(define (handler-lookup #:channel channel #:nodeid nodeid #:name name #:reply reply-entry #:error error)
  (begin
    (displayln (string-append "inode: " (~a nodeid)))
    (displayln (string-append "lookup name: " (path->string name)))
    (reply-entry #:generation 0 #:entry-valid  (timespec 1 0) #:attr-valid  (timespec 1 0)
                 #:inode 2 #:rdev 0 #:size 13 #:blocks 1
                 #:atime  (timespec 1381237736 0) #:mtime  (timespec 1381237736 0)
                 #:ctime  (timespec 1381237736 0) #:kind 'S_IFREG
                 #:perm '(S_IRUSR S_IWUSR S_IRGRP S_IROTH)
                 #:nlink 1 #:uid 1000 #:gid 1000)
    (error 'ENOENT)
    )
  )

(define (handle-init http-request #:channel channel)
  (define root-id (get-root-id http-request))
  (begin
    (displayln "init")
    (displayln (string-append "root_id: " root-id))
    (async-channel-put channel (node (hash 1 root-id) (hash root-id 1) 2))
    #t))

(define (content-to-dentry content
                           node-id-to-folder-id
                           folder-id-to-node-id
                           next-node-id
                           offset
                           reply-add
                           reply-done)
  (define (add-file name inode offset) (reply-add #:inode inode #:offset offset #:kind 'S_IFREG #:name (string->path name)))
  (define (add-folder name inode offset) (reply-add #:inode inode #:offset offset #:kind 'S_IFDIR #:name (string->path name)))
  (match content
         [(list-rest head tail) (match head
                                       [(file name _ entry-id) (if (hash-has-key? folder-id-to-node-id entry-id)
                                                                 (begin
                                                                   (displayln (string-append "filename: " name))
                                                                   (add-file name (hash-ref folder-id-to-node-id entry-id) offset)
                                                                   (content-to-dentry tail
                                                                                      node-id-to-folder-id
                                                                                      folder-id-to-node-id
                                                                                      next-node-id
                                                                                      (add1 offset)
                                                                                      reply-add
                                                                                      reply-done))
                                                                 (begin
                                                                   (displayln (string-append "filename: " name))
                                                                   (add-file name next-node-id offset)
                                                                   (content-to-dentry tail
                                                                                      (hash-set node-id-to-folder-id next-node-id entry-id)
                                                                                      (hash-set folder-id-to-node-id entry-id next-node-id)
                                                                                      (add1 next-node-id)
                                                                                      (add1 offset)
                                                                                      reply-add
                                                                                      reply-done)))]
                                       [(folder name _ folder-id) (if (hash-has-key? folder-id-to-node-id folder-id)
                                                                    (begin
                                                                   (displayln (string-append "folder name " name))
                                                                      (add-folder name (hash-ref folder-id-to-node-id folder-id) offset)
                                                                      (content-to-dentry tail
                                                                                         node-id-to-folder-id
                                                                                         folder-id-to-node-id
                                                                                         next-node-id
                                                                                         (add1 offset)
                                                                                         reply-add
                                                                                         reply-done))
                                                                    (begin
                                                                   (displayln (string-append "folder name " name))
                                                                      (add-folder name next-node-id offset)
                                                                      (content-to-dentry tail
                                                                                         (hash-set node-id-to-folder-id next-node-id folder-id)
                                                                                         (hash-set folder-id-to-node-id folder-id next-node-id)
                                                                                         (add1 next-node-id)
                                                                                         (add1 offset)
                                                                                         reply-add
                                                                                         reply-done)))]
                                       )]
         [empty (begin
                  (displayln "Empty list")
                  (reply-done)
                  (node node-id-to-folder-id folder-id-to-node-id next-node-id))]))

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
  (match current-folder-id
         [(nothing) (begin
                    (displayln (string-append "no entry for nodeid: " (~a nodeid)))
                    (async-channel-put channel node-id-cache)
                    (error 'ENOENT))]
         [(just id) (begin
                      (displayln (string-append "found entry for nodeid: " (~a id)))
                      (let ([node (content-to-dentry (list-folder http-request id) nid-to-fid fid-to-nid next-id 1 reply-add reply-done)])
                          (async-channel-put channel node)))]))

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
  (let ([work-channel (make-async-channel 1)])
      (make-filesystem #:lookup (partial handler-lookup #:channel work-channel)
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
