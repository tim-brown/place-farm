#lang racket

(require racket/place)

(provide place-farm)

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
  (let place-farm-loop ((work-to-do work-to-do)
                        (plcs null)
                        (acc result-fold-nil)
                        (places-hash (hash)))
    (log-debug "place-farm-loop: ~s" (list (length work-to-do) plcs acc places-hash))
    (cond
      [(and (pair? work-to-do) (< (length plcs) farm-size))
       (log-debug "More work")
       (let* ((i (caar work-to-do))
              (p (make-worker)))
         (for [(msg (car work-to-do))]
           (place-channel-put p msg))
         (log-info "work `~a` assigned to ~a" i p)
         (place-farm-loop (cdr work-to-do) (cons i plcs) acc
                          (hash-set places-hash i p)))]
      
      [(and (null? work-to-do) (null? plcs))
       (log-info "All done!")     
       acc]
      
      [(and (log-debug "loop: current-places are: ~s" plcs) #f) #f]
      
      [(apply sync/timeout farm-timeout/s
              (map (lambda (id) (hash-ref places-hash id)) plcs))
       =>
       (lambda (plc-id)
         (log-debug "synched on: ~a" plc-id)
         (let* ((id (car plc-id))
                (rv (cdr plc-id))
                (plc (hash-ref places-hash id)) 
                (new-plcs (remove id plcs))
                (new-acc (result-fold-cons rv acc))
                (new-hsh (hash-remove places-hash id)))
           (place-channel-put plc 'shoo)
           (log-debug "waiting: ~a [new# ~a]" plc new-hsh)
           (place-wait plc)
           (place-farm-loop work-to-do new-plcs new-acc new-hsh)))]
      
      [else (log-info "timeout: ~a" (list (length work-to-do) plcs acc places-hash))
            (place-farm-loop work-to-do plcs acc places-hash)]))))

(define-syntax-rule
  (define-make-place-farm-worker id ch . content)
  (define (id)
    (place ch
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

  ;; removed from test suite because it takes 30s
  ;; cpu time: 29778 real time: 29761 gc time: 0
  ;; (time (map sub-optimal-square place-farm-input))

  ;; farm-size: 4 -- cpu time: 39415 real time: 13822 gc time: 0
  ;; ooh -- about 1/4 the time (so all four of my processors were used
  (check-match
    (time
      (apply place-farm 8 make-squarer-place null
             (lambda (a d) (append d (list a)))
             (map (lambda (x) (list x x)) place-farm-input)))
    '(1000000 4000000 9000000 16000000 25000000 36000000 49000000 64000000
      81000000 100000000))
  )

; vim: syntax=racket
