#lang racket
(provide
 place-farm
 define-make-place-farm-worker)

(require racket/place)
(define-logger place-farm)

(define/contract
  (place-farm
   farm-size
   make-worker
   result-fold-nil result-fold-cons
   #:farm-timeout/s (farm-timeout/s #f)
   . work-to-do)
  
  (->* (exact-nonnegative-integer? (-> place?) any/c any/c)
       (#:farm-timeout/s (or/c #f (and/c real? (not/c negative?))))
       #:rest (listof (listof any/c))
       any/c)
  
  (parameterize ((current-logger place-farm-logger))    
    (define (place-farm-loop work-to-do plcs acc places-hash)
      
      (define (handle-place-sync plc-id)
        (log-debug "synched on: ~a" plc-id)
        (match-define (cons id rv) plc-id)
        (define plc (hash-ref places-hash id))
        (define plcs- (remove id plcs))
        (define acc+ (result-fold-cons rv acc))
        (define places-hash- (hash-remove places-hash id))        
        (place-channel-put plc 'shoo)
        (log-debug "waiting: ~a [new# ~a]" plc places-hash-)
        (place-wait plc)
        (place-farm-loop work-to-do plcs- acc+ places-hash-))
      
      (define (assign-work)
        (log-debug "More work")
        (match-define (list-rest (and msgs (cons id place-args)) more-work-to-do) work-to-do)
        (define p (make-worker))
        (for [(msg (in-list msgs))] (place-channel-put p msg))
        (log-info "work `~a` assigned to ~a" id p)
        (place-farm-loop more-work-to-do (cons id plcs) acc (hash-set places-hash id p)))
      
      (define (all-done)
        (log-info "All done!")     
        acc)
      
      (define (timeout/recur)
        (log-info "timeout: ~a" (list (length work-to-do) plcs acc places-hash))
        (place-farm-loop work-to-do plcs acc places-hash))
      
      (log-debug "place-farm-loop: ~s" (list (length work-to-do) plcs acc places-hash))
      (cond
        [(and (pair? work-to-do) (< (length plcs) farm-size)) (assign-work)]        
        [(and (null? work-to-do) (null? plcs)) (all-done)]        
        [(and (log-debug "loop: current-places are: ~s" plcs) #f) #f]        
        [(apply sync/timeout farm-timeout/s
                (for/list ((p-id plcs)) (hash-ref places-hash p-id)))
         => handle-place-sync]        
        [else (timeout/recur)]))
    (place-farm-loop work-to-do null result-fold-nil (hash))))

(define-syntax-rule
  (define-make-place-farm-worker id ch . content)
  (define (id)
    (place
     ch
     (log-debug "worker: Waiting for id on: ~a" ch)
     (define my-id (place-channel-get ch))
     (log-debug "worker: Got an id: ~a" my-id)
     (define (rv my-id) . content)
     (place-channel-put ch (cons my-id (rv my-id))))))

; Hmm... I recall that places in sub-modules don't work so well
; that's why these two are in the toplevel module (and not `test`)
(define (sub-optimal-square n)
  (printf "(sub-optimal-square ~a)~%" n)
  (for*/sum ((i n) (j n))
    1))
(define-make-place-farm-worker
  make-squarer-place ch
  (define n (place-channel-get ch))
  (sub-optimal-square n))  

(define-make-place-farm-worker
  make-batch-squarer-place ch
  (define start-inc (place-channel-get ch))
  (define end-excl (place-channel-get ch))
  (for/list ((i (in-range start-inc end-excl)))
    (sub-optimal-square i)))


(module+ test
  (require rackunit)
  
  (check-= (sub-optimal-square 4) 16 0 "quick sub-optimal-square check") 
  (define m-s-p (make-squarer-place))
  (place-channel-put m-s-p 'woo)
  (place-channel-put m-s-p 3)
  (check-equal? '(woo . 9) (place-channel-get m-s-p))
  
  ;; basic use
  (define place-farm-result
    (place-farm 4 make-squarer-place null
                (lambda (a d) (append d (list a)))
                '(a 1) '(b 2) '(c 3) '(d 4) '(e 5) '(f 6) '(g 7) '(h 8) '(i 9) '(j 10)))
  (check-equal? '(1 4 9 16 25 36 49 64 81 100) (sort place-farm-result <))
  
  ;; slightly more likely use
  (define place-farm-input '(1000 2000 3000 4000 5000 6000 7000 8000 9000 10000))

  
  (define place-farm-batch-input '((0 10) (10 20) (20 30) (30 40)))

  ;; removed from test suite because it takes 30s
  ;; cpu time: 29778 real time: 29761 gc time: 0
  ;; (time (map sub-optimal-square place-farm-input))
  
  ;; farm-size: 4 -- cpu time: 39415 real time: 13822 gc time: 0
  ;; ooh -- about 1/4 the time (so all four of my processors were used
  (check-match
   (time
    (sort (apply place-farm 8 make-squarer-place null
                 (lambda (a d) (append d (list a)))
                 (map (lambda (x) (list x x)) place-farm-input)) <))
   '(1000000 4000000 9000000 16000000 25000000 36000000 49000000 64000000 81000000 100000000))
  
  (check-match
   (time
    (sort (apply place-farm 2 make-batch-squarer-place null
                 (lambda (a d) (append d a))
                 (for/list ((i (in-naturals)) (x (in-list place-farm-batch-input))) (cons i x)))
                 <))
   '(0 1 4 9 16 25 36 49 64 81 100 121 144 169 196 225 256 289 324 361
       400 441 484 529 576 625 676 729 784 841 900 961 1024 1089 1156 1225 1296 1369 1444 1521))
  
  )

; vim: syntax=racket
