#lang racket

(require racket/cmdline)
(require fuse)
(require kw-utils/partial)
(require racket/async-channel)
(require net/http-client)
(require json)
(require racket/match)
(require racket/port)

(struct folder (name last-modified folder-id))
(struct file (name last-modified entry-id))
(struct http-response (status headers body))
(struct http-request (get))

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
  ((to-content-list (hash-ref content 'folders) (hash-ref content 'files))))

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
    (displayln (async-channel-get channel))
    (async-channel-put channel "test2")
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
  (begin
    (displayln "init")
    (displayln (string-append "root_id: " (get-root-id http-request)))
    (async-channel-put channel "test")
    #t
    )
  )

(define (readdir #:nodeid nodeid #:info info #:offset offset #:add reply-add #:reply reply-done #:error error)
  (begin
    (displayln (string-append "trying to read dir, offset: " (~a offset)))
    (if (= nodeid 1)
      (begin
        (when (zero? offset)
          (reply-add #:inode 1 #:offset 1 #:kind 'S_IFREG #:name (string->path "."))
          (reply-add #:inode 1 #:offset 2 #:kind 'S_IFREG #:name (string->path "..")))
        (reply-done))
      (error 'ENOENT)))
)

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
                       #:readdir readdir
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
