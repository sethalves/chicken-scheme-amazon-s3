; author: Thomas Hintz
; email: t@thintz.com
; license: bsd

(use test amazon-s3)

(define *b* "chicken-scheme-test-bucket-1")
(access-key "AKIAJS2UGBOTG36AKH3A")
(secret-key "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")

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
 (test-assert "Delete Object" (delete-object! *b* "key"))
 (test-assert "Put String" (put-string! *b* "string" "res-string"))
 (test "Get String" "res-string" (get-string *b* "string"))
 (test-assert "Delete Object 2" (delete-object! *b* "string"))
 (test-assert "Put Sexp" (put-sexp! *b* "sexp" '(+ 1 2 3)))
 (test "Get Sexp" 6 (eval (get-sexp *b* "sexp")))
 (test-assert "Put File" (put-file! *b* "file" "file"))
 (test-assert "Get File" (get-file *b* "file" "test-out-file"))
 (test "Get/Put File 1" #t
       (string=?
        (with-input-from-file "file" (lambda () (read-string)))
        (with-input-from-file "test-out-file" (lambda () (read-string)))))
 (test-assert "Delete Object 3" (delete-object! *b* "sexp"))
 (test-assert "Delete Object 4" (delete-object! *b* "file"))
 (test-assert "Delete Bucket" (delete-bucket! *b*)))

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
;;         "s3:PutObject"
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
