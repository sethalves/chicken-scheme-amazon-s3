; author: Thomas Hintz <t@thintz.com>
; maintainer: Seth Alves <seth@hungry.com>
; license: bsd

(module amazon-s3
  (;; debugging
   *last-sig*

   ;; params
   access-key secret-key https make-base-uri

   ;; procs
   list-objects
   list-buckets
   bucket-exists?
   create-bucket!
   delete-bucket!
   object-exists?
   get-object
   put-object!
   delete-object!
   put-string!
   put-sexp!
   put-file!
   get-string
   get-sexp get-file

   ;; macros
   with-bucket)

(import scheme chicken srfi-1 extras srfi-13 data-structures ports posix)
(use base64 sha1 http-client uri-common intarweb srfi-19 hmac ssax sxpath)

; needed to make intarweb work with Amazon's screwy authorization header
(define authorization-unparser
  (alist-ref 'authorization (header-unparsers)))

(define (aws-authorization-unparser header-contents)
  (map (lambda (header)
         (if (eq? (get-value header) 'aws)
             (let ((params (get-params header)))
               ;; Some servers (Ceph) insist on the "AWS" auth-scheme
               ;; being all caps even though the framework laid out by
               ;; RFC2617 says it's supposed to be case insensitive!
               (sprintf "AWS ~A:~A"
                 (alist-ref 'access-key params)
                 (alist-ref 'signed-secret params)))
             (authorization-unparser (list header))))
       header-contents))

(header-unparsers
 `((authorization . ,aws-authorization-unparser) . ,(header-unparsers)))

;;; params

(define (intarweb-date date)
  (string->time (date->string date "~a ~b ~d ~T ~Y GMT") "%a %b %d %T %Y %Z"))
(define (sig-date date) (date->string date "~a, ~d ~b ~Y ~T GMT"))

(define access-key (make-parameter ""))
(define secret-key (make-parameter ""))

;; DEPRECATED
(define https (make-parameter #t))

(define make-base-uri
  (make-parameter
   (lambda (bucket)
     (make-uri scheme: (if (https) 'https 'http)
               host: (if bucket
                         (string-append bucket "." "s3.amazonaws.com")
                         "s3.amazonaws.com")))))

;;; helper methods

(define (assert-404 exn)
  (if (string=? ((condition-property-accessor 'exn 'message) exn)
                 "Client error: 404 Not Found")
       #f
       (abort exn)))

(define (make-aws-authorization verb resource
                                #!key
                                (date #f)
                                (amz-headers '())
                                (content-md5 #f)
                                (content-type #f))
  (let* ((can-amz-headers
          (sort (map (lambda (header)
                       `(,(string-downcase (car header)) . ,(cdr header)))
                     amz-headers)
                (lambda (v1 v2)
                  (string<? (car v1) (car v2)))))
         (can-string
          (string-append
           (string-upcase verb) "\n"
           (if content-md5 content-md5 "") "\n"
           (if content-type content-type "") "\n"
           (if date date "") "\n"
           (fold (lambda (e o)
                   (string-append o (sprintf "~a:~a~%" (car e) (cdr e))))
                 ""
                 can-amz-headers)
           resource))
         (hmac-sha1 (base64-encode
                     ((hmac (secret-key) (sha1-primitive)) can-string))))
    (set! *last-sig* can-string)
    (values hmac-sha1 can-string)))


(define *last-sig* #f)
(define amazon-ns
  (make-parameter '(x . "http://s3.amazonaws.com/doc/2006-03-01/")))


(define (aws-headers bucket path verb content-type content-length acl)
  (let ((n (current-date 0)))
    (let-values (((hmac-sha1 can-string)
                  (make-aws-authorization
                   verb
                   (string-append "/"
                                  (if bucket (string-append bucket "/") "")
                                  (or path ""))
                   date: (sig-date n)
                   content-type: content-type
                   amz-headers: (if acl (list (cons "X-Amz-Acl" acl)) '()))))
      (headers `((date #(,(intarweb-date n) ()))
                 (authorization #(aws ((access-key . ,(access-key))
                                       (signed-secret . ,hmac-sha1))))
                 ,@(if acl `((x-amz-acl ,acl)) '())
                 (content-type ,(string->symbol content-type))
                 (content-length ,content-length))))))


(define (aws-request bucket path verb query
                     #!key
                     no-auth
                     (content-type "")
                     (content-length 0)
                     (acl #f))
  (let* ((base ((make-base-uri) bucket))
         (path-ref (uri-reference path))
         (uri/path (if path-ref (uri-relative-to path-ref base) base))
         (final-uri (update-uri uri/path query: query)))
    (make-request
     method: (string->symbol verb)
     uri: final-uri
     headers: (if no-auth (headers '())
                  (aws-headers bucket path verb
                               content-type content-length acl)))))


(define (aws-xml-parser path ns)
  (lambda ()
     ((sxpath path)
      (ssax:xml->sxml (current-input-port) ns))))

(define (perform-aws-request
         #!key
         (bucket #f)
         (path #f)
         (query #f)
         (sxpath '())
         (body "")
         (verb "GET")
         (ns '((x . "http://s3.amazonaws.com/doc/2006-03-01/")))
         (no-xml #f)
         (no-auth #f)
         (reader-thunk read-string)
         (content-type "application/x-www-form-urlencoded")
         (content-length 0)
         (acl #f))
  (with-input-from-request
   (aws-request bucket path verb query no-auth: no-auth
                content-type: content-type content-length: content-length
                acl: acl)
   body
   (if no-xml
       reader-thunk
       (aws-xml-parser sxpath ns))))


(define (read-byte-file path . port)
  (lambda ()
    (let ((file (open-input-file path)))
      (letrec ((read-next
                (lambda ()
                  (let ((b (read-byte file)))
                    (if (eof-object? b)
                        #t
                        (begin
                          (if (> (length port) 0)
                              (write-byte b (car port))
                              (write-byte b))
                          (read-next)))))))
        (read-next))
      (close-input-port file))))


(define (write-byte-file path . port)
  (lambda ()
    (let ((file (open-output-file path)))
      (letrec ((read-next
                (lambda ()
                  (let ((b (if (> (length port) 0)
                               (read-byte (car port))
                               (read-byte))))
                    (if (eof-object? b)
                        #t
                        (begin 
                          (write-byte b file)
                          (read-next)))))))
        (read-next))
      (close-output-port file))))


;;; api

; broken and deprecated
; next version will have parameterized keywords so this
; won't be necessary
(define-syntax with-bucket
  (syntax-rules ()
    ((with-bucket bucket (func p1 ...))
     (func bucket p1 ...))
    ((with-bucket bucket exp body ...)
     (begin (with-bucket bucket exp)
            (with-bucket bucket body ...)))))


(define (list-buckets)
  (perform-aws-request
   sxpath: '(x:ListAllMyBucketsResult x:Buckets x:Bucket x:Name *text*)))


(define (bucket-exists? bucket)
  (handle-exceptions
   exn
   (assert-404 exn)
   (perform-aws-request bucket: bucket verb: "HEAD" no-xml: #t)
   #t))


(define (create-bucket! bucket)
  (perform-aws-request bucket: bucket verb: "PUT" no-xml: #t))


(define (delete-bucket! bucket)
  (perform-aws-request bucket: bucket verb: "DELETE" no-xml: #t)
  #t)


(define (list-objects bucket #!key prefix)
  ;; TODO: Somehow(?) handle "IsTruncated" results and add support for
  ;; "marker" so one can page through the results.  Possibly this can
  ;; return a generator procedure or accept a callback to handle the
  ;; listing.
  (perform-aws-request
   bucket: bucket query: `((prefix . ,prefix))
   sxpath: '(x:ListBucketResult x:Contents x:Key *text*)))


(define (object-exists? bucket key)
  (handle-exceptions
   exn
   (assert-404 exn)
   (perform-aws-request bucket: bucket path: key verb: "HEAD" no-xml: #t)
   #t))


(define (put-object! bucket key object-thunk object-length object-type
                     #!key (acl #f))
  (perform-aws-request bucket: bucket path: key verb: "PUT"
                       content-type: object-type body: object-thunk
                       content-length: object-length no-xml: #t acl: acl))


(define (put-string! bucket key string #!key (acl #f))
  (put-object! bucket key
               (lambda () (display string)) (string-length string)
               "text/plain" acl: acl))


(define (put-sexp! bucket key sexp #!key (acl #f))
  (let-values (((res request-uri response)
                (put-string! bucket key (->string sexp) acl: acl)))
    (values res request-uri response)))


(define (put-file! bucket key file-path #!key (acl #f))
  (put-object! bucket key (read-byte-file file-path)
               (file-size file-path) "binary/octet-stream" acl: acl))


(define (get-object bucket key)
  (perform-aws-request bucket: bucket path: key no-xml: #t))


(define (get-string bucket key)
  (perform-aws-request bucket: bucket path: key no-xml: #t))


(define (get-sexp bucket key)
  (let-values (((string request-uri response) (get-string bucket key)))
    (values (call-with-input-string string read) request-uri response)))


(define (get-file bucket key file-path)
  (perform-aws-request
   bucket: bucket path: key no-xml: #t
   reader-thunk: (write-byte-file file-path)))


(define (delete-object! bucket key)
  (perform-aws-request bucket: bucket path: key no-xml: #t verb: "DELETE")
  #t)

)
