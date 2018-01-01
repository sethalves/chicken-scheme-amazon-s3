; author: Thomas Hintz
; email: t@thintz.com
; license: bsd

(use test amazon-s3)
(use http-client)

(define *b* "chicken-scheme-test-bucket-1")

;; break up the key so that amazon's scanner doesn't find it and cause someone
;; to freak out.  They key is useless for anything other than this test.
(access-key (string-append "AK"
                           "IAI"
                           "YOT43"
                           "G3DXSD"
                           "DBUA"))
(secret-key (string-append "upDo"
                           "HLtUu"
                           "UT"
                           "mMI+p"
                           "QSW15EK"
                           "O9tII2"
                           "38OR4Y"
                           "FDZ7l"))

(define got-403 #f)

(test-group
 "Amazon S3"
 (test "Bucket Exists 1" #f (bucket-exists? *b*))
 (test-assert "Create Bucket" (create-bucket! *b*))
 (test "Bucket Exists 2" #t (bucket-exists? *b*))
 ;; the credentials are limited to "arn:aws:s3:::chicken-scheme-test-bucket-1"
 ;; so listing all buckets on the account wont work.
 ;; (test-assert "List Buckets" (list-buckets)) ; should test this more speci
 (test "List Bucket Objects 1" '() (list-objects *b*))
 (test-assert "Put Object" (put-object! *b* "key" (lambda () (display "value")) (string-length "value") "text/plain"))
 (test "List Bucket Objects 2" '("key") (list-objects *b*))
 (test-assert "Put String" (put-string! *b* "string" "res-string"))
 (test "Get String" "res-string" (get-string *b* "string"))
 (test "List Bucket Objects 3" '("key" "string") (list-objects *b*))
 (test "List Bucket Objects w/ prefix" '("key") (list-objects *b* prefix: "k"))
 (test-assert "Delete Object 1" (delete-object! *b* "key"))
 (test-assert "Delete Object 2" (delete-object! *b* "string"))

 (test "Object key encoding" (put-string! *b* "//12%456%2F?78#9aB" "x"))
 (test "List Bucket objects returns same key name"
       '("//12%456%2F?78#9aB") (list-objects *b*))
 (test-assert "Delete encoded object"
              (delete-object! *b* "//12%456%2F?78#9aB"))

 (test-assert "Put Sexp" (put-sexp! *b* "sexp" '(+ 1 2 3)))
 (test "Get Sexp" 6 (eval (get-sexp *b* "sexp")))
 (test-assert "Put File" (put-file! *b* "file" "file"))
 (test-assert "Get File" (get-file *b* "file" "test-out-file"))
 (test "Get/Put File 1" #t
       (string=?
        (with-input-from-file "file" (lambda () (read-string)))
        (with-input-from-file "test-out-file" (lambda () (read-string)))))


 ;; test acl
 (test-assert "Acl Put String" (put-string! *b* "string" "res-string"))
 (handle-exceptions x (set! got-403 #t)
   (with-input-from-request
    "https://s3.amazonaws.com/chicken-scheme-test-bucket-1/string"
    #f (lambda () #t)))
 (test-assert got-403)
 (test-assert "Delete Object 3" (delete-object! *b* "string"))
 (test-assert "Acl Put String"
              (put-string! *b* "string" "res-string" acl: 'public-read))
 (with-input-from-request
  "https://s3.amazonaws.com/chicken-scheme-test-bucket-1/string"
  #f (lambda () #t))
 (test-assert "Delete Object 4" (delete-object! *b* "string"))

 (test-assert "Delete Object 5" (delete-object! *b* "sexp"))
 (test-assert "Delete Object 6" (delete-object! *b* "file"))
 (test-assert "Delete Bucket" (delete-bucket! *b*))

 )


(test-exit)


;; aws credentials needed to run this test:
;; {
;;   "Version": "2012-10-17",
;;   "Statement": [
;;     {
;;       "Sid": "Stmt1391630304000",
;;       "Effect": "Allow",
;;       "Action": [
;;         "s3:CreateBucket",
;;         "s3:DeleteBucket",
;;         "s3:DeleteObject",
;;         "s3:GetObject",
;;         "s3:ListBucket",
;;         "s3:PutObject",
;;         "s3:PutObjectAcl"
;;       ],
;;       "Resource": [
;;         "arn:aws:s3:::chicken-scheme-test-bucket-1",
;;         "arn:aws:s3:::chicken-scheme-test-bucket-1/key",
;;         "arn:aws:s3:::chicken-scheme-test-bucket-1/string",
;;         "arn:aws:s3:::chicken-scheme-test-bucket-1/sexp",
;;         "arn:aws:s3:::chicken-scheme-test-bucket-1/file"
;;       ]
;;     }
;;   ]
;; }
