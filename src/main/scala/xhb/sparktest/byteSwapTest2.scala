package xhb.sparktest
import scala.util.hashing
object byteSwapTest2 extends App{
  val shift=1;
  val idx=1;
  val seed = byteswap32(idx ^ (shift << 16))
  def byteswap32(v: Int): Int = {
    var hc = v * 0x9e3775cd
    hc = java.lang.Integer.reverseBytes(hc)
    hc * 0x9e3775cd
  }
  println((shift<<16).toBinaryString)
  println((idx^(shift<<16)).toBinaryString)
  println((((shift<<16)^idx)*0x9e3775cd).toBinaryString)
  println(seed.toBinaryString)
}