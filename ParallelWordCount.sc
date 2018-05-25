import scala.io.Source
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveTask
import java.util.concurrent.ForkJoinWorkerThread
import java.util.concurrent._

import scala.util.Random

object Main {
  val forkJoinPool = new ForkJoinPool

  def task[T](computation: => T): RecursiveTask[T] = {
    val t = new RecursiveTask[T] {
      def compute = computation
    }

    Thread.currentThread match {
      case wt: ForkJoinWorkerThread =>
        t.fork() // schedule for execution
      case _ =>
        forkJoinPool.execute(t)
    }

    t
  }

  def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
    val right = task { taskB }
    val left = taskA

    (left, right.join())
  }

  def parallel[A, B, C, D](taskA: => A, taskB: => B, taskC: => C, taskD: => D): (A, B, C, D) = {
    val ta = task { taskA }
    val tb = task { taskB }
    val tc = task { taskC }
    val td = taskD
    (ta.join(), tb.join(), tc.join(), td)
  }
  
  //////////////////

  trait Monoid[A] {
    def op(x: A, y: A): A
    def zero: A
  }
  
  def foldMapPar[A, B](xs: IndexedSeq[A],
                       from: Int, to: Int,
                       m: Monoid[B])
                       (f: A => B)
                       (implicit thresholdSize: Int): B = 
    if (to - from <= thresholdSize)
      foldMapSegment(xs, from, to, m)(f)
    else {
      val middle = from + (to - from) / 2
      val (l, r) = parallel(
          foldMapPar(xs, from, middle, m)(f)(thresholdSize),
          foldMapPar(xs, middle, to, m)(f)(thresholdSize))
          
      m.op(l, r)
    }
  
  def foldMapSegment[A, B](xs: IndexedSeq[A],
                           from: Int, to: Int,
                           m: Monoid[B])
                           (f: A => B): B = {
    var res = f(xs(from))
    var index = from + 1
    while (index < to) {
      res = m.op(res, f(xs(index)))
      index = index + 1
    }
    res
  }
  
  //////////////////
  
  // Everything that's not alphanumeric is considered a whitespace or punctuation
  def isAlphaNum(c: Char) =
    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '1')
  
  // NOTE: We don't need to store the whole word on the left and right. We only need to know if it's there.
  // So I represent these words with Int:
  //     0 - the string starts/ends with a whitespace or punctuation
  //     1 - the string starts/ends with an alphanumeric character
  case class WordCountTuple(leftWord: Int, count: Int, rightWord: Int)
  
  def wordCount(text: String) = {
    
    // This is a string stream
    // It imitates the behavior of Source file stream
    object stream {
      var index = 0
      def hasNext = index < text.size
      
      def next = {
        index += 1
        text(index - 1)
      }
    }
    
    // Finite state automaton:
    // s0 - initial state, this function. Read the first character
    // s1 - text starts with word (first word, may be part of word)
    // s2 - space or punctuation
    // s3 - word after space or punctuation, if we go to s2 from here, we increment count
    // s4 - final state, we go there if we return something
      
    def s1(): WordCountTuple = {
      if (stream.hasNext) {
        val c = stream.next
        if (isAlphaNum(c)) s1()
        else s2(1, 0)
      }
      else WordCountTuple(1, 0, 1)
    }
     
    def s2(firstWord: Int, count: Int): WordCountTuple = {
      if (stream.hasNext) {
        val c = stream.next
        if (isAlphaNum(c)) s3(firstWord, count)
        else s2(firstWord, count)
      }
      else WordCountTuple(firstWord, count, 0)
    }
     
    def s3(firstWord: Int, count: Int): WordCountTuple = {
      if (stream.hasNext) {
        val c = stream.next
        if (isAlphaNum(c)) s3(firstWord, count)
        else s2(firstWord, count + 1)
      }
      else WordCountTuple(firstWord, count, 1)
    }
      
    if (stream.hasNext) {
      val c = stream.next
      if (isAlphaNum(c)) s1()
      else s2(0, 0)
    }
    else WordCountTuple(0, 0, 0)
  }
    
  def main(args: Array[String]): Unit = {
    
    val bufferedSource = Source.fromFile("/Users/oleks/Desktop/lipsum.txt")
    val text = bufferedSource.mkString.grouped(50).toVector
    bufferedSource.close

    implicit val threshold = 100
                              
    def max(a: Int, b: Int) =
      if (a >= b) a
      else b
    
    val monoid = new Monoid[WordCountTuple] {
      def op(x: WordCountTuple, y: WordCountTuple) =
        WordCountTuple(x.leftWord, x.count + y.count + max(x.rightWord, y.leftWord), y.rightWord)
        
      def zero = WordCountTuple(0, 0, 0)
    }
    
    val res = foldMapPar(text, 0, text.length, monoid)(wordCount)
      
    println(res)
  }
}