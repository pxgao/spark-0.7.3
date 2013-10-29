package spark.streaming.examples

import scala.util.parsing.combinator.syntactical._
import scala.io.Source

import scala.util.parsing.combinator._

abstract class Evalable extends Serializable{
  def Eval(col : Map[String,Int], rec : IndexedSeq[Any]): Any
}


class SelectStatement(s : Map[String,Any]) {
  val sql = s
  override def toString = sql.toString()
}

class InputStatement(i : String, p : Int, d : String, c : Map[String, String] ){
  val ip = i
  val port = p
  val dilimiter = d
  val column = c
  override def toString = "input "+column+" from "+ip+":"+port+" delimiter "+dilimiter
}

class OutputStatement(t : String, d : String, c : List[SelectItem]){
  val table = t
  val delimiter = d
  val column = c
  override def toString = "output "+column+" delimiter " + delimiter
}

class WindowProperty( wd :Int, sd: Int){
  val windowDuration = wd
  val slideDuration = sd
  override def toString = "Window:" + windowDuration + "," + slideDuration
}

class Condition( l:Evalable, op:String, r:Evalable) extends Serializable{
  val o = op
  val left = l
  val right = r
  def Eval(col : Map[String,Int], rec : IndexedSeq[Any]) : Boolean = {
    val o = this.o
    val left = this.left
    val right = this.right
    val leftValue = left.Eval(col,rec)
    val rightValue = right.Eval(col, rec)
    if((leftValue.isInstanceOf[Double] || leftValue.isInstanceOf[Int])
      && (rightValue.isInstanceOf[Double] || rightValue.isInstanceOf[Int])){
      val leftDouble : Double = leftValue match{
        case leftValue : Int => leftValue.toDouble
        case leftValue : Double => leftValue
      }
      val rightDouble : Double = rightValue match{
        case rightValue : Int => rightValue.toDouble
        case rightValue : Double => rightValue
      }
      o match{
        case "=" => leftDouble == rightDouble
        case "<" => leftDouble < rightDouble
        case ">" => leftDouble > rightDouble
        case ">=" => leftDouble >= rightDouble
        case "<=" => leftDouble >= rightDouble
        case "<>" => leftDouble != rightDouble
      }
    }
    else if(leftValue.isInstanceOf[String] && rightValue.isInstanceOf[String]){
      o match{
        case "=" => leftValue.asInstanceOf[String] == rightValue.asInstanceOf[String]
        case "<>" => leftValue.asInstanceOf[String] != rightValue.asInstanceOf[String]
        case _ => throw new Exception("Unsupported operation for String")
      }
    }else{
      throw new Exception("Type mismatch")
    }
  }
  def GetVariableSet() : Set[String] = {
    val e = Set[Evalable](left, right)
    e.filter(_.isInstanceOf[Identifier]).map(_.asInstanceOf[Identifier].name)
  }
  override def toString = left.toString() + o + right.toString()
}

class SelectItem(tab:Identifier, col : Identifier, f : Identifier, newName : Identifier){
  val table = tab
  val selectCol = col
  val function = f
  val asName = newName
  override def toString = f + "(" + table + "." + selectCol + ") as " + asName;
}

class JoinStatement(jtype : String, tab : Identifier, on : List[(ColOfTable,ColOfTable)]){
  val joinType = jtype
  val table = tab
  val onCol = on
  override def toString = joinType + " " + table + " on " + onCol;
}

class ColOfTable(tab: Identifier,col : Identifier){
  val column = col
  val table = tab
  override def toString = tab + "." + column
}

class Identifier(s : String) extends Evalable{
  val name = s
  override def Eval(col : Map[String,Int], rec : IndexedSeq[Any]) : Any = rec(col(s))
  override def toString = name
}

class Float(s : String) extends Evalable{
  val f = s.toDouble
  override  def Eval(col : Map[String,Int], rec : IndexedSeq[Any]) = f
  override def toString = f.toString
}

class Integer(s : String) extends Evalable{
  val i = s.toInt
  override def Eval(col : Map[String,Int], rec : IndexedSeq[Any]) = i
  override def toString = i.toString
}

class StringValue(s : String) extends Evalable{
  val i = s
  override def Eval(col : Map[String,Int], rec : IndexedSeq[Any]) : Any = rec(col(s))
  override def toString = i.toString
}

class Comment(s : String){
  val comment = s
  override def toString = comment
}


class SqlParser extends JavaTokenParsers {

  def line : Parser[Any] = query | output_statement | comment

  def comment : Parser[Comment] =
    """//.*""".r ^^ {case s => new Comment(s)}

  def query : Parser[(Identifier, Any)] =
    identifier~"="~(select_statement | input_statement) ^^
      {
        case i~"="~s => (i,s)
      }

  def input_statement : Parser[InputStatement] =
    "input"~repsep(identifier~":"~dataType, ",")~"from"~iAddress~":"~integer~"delimiter"~delimiter ^^
      {case "input"~c~"from"~i~":"~p~"delimiter"~d => new InputStatement(i,p.i,d,c.map(i => (i._1._1.name,i._2)).toMap)}

  def output_statement : Parser[OutputStatement] =
    "output"~repsep(identifier, ",")~"from"~identifier~"delimiter"~delimiter ^^
      {case "output"~c~"from"~t~"delimiter"~d => new OutputStatement(t.name,d,c.map(i => new SelectItem(null,i,null,null)))}

	def select_statement : Parser[SelectStatement] =
		select~from~rep(clause) ^^
		{
			case s~f~c => {
        var clauses = Map[String, Any]()
        c.foreach(kvp => {
          if(kvp._1 == "where" || kvp._1 == "groupby" || kvp._1 == "window")
            if(!clauses.contains(kvp._1))
              clauses += kvp
            else
              failure("Duplicated " + kvp._1)
          else if(kvp._1 == "join")
            if(!clauses.contains(kvp._1))
              clauses += kvp._1 -> List(kvp._2)
            else
            {
              val oldList = clauses(kvp._1).asInstanceOf[List[JoinStatement]]
              clauses -= kvp._1
              clauses += kvp._1 -> (oldList :+ kvp._2)
            }
        })
        new SelectStatement(Map() + s + f ++ clauses)
      }
		}

	//need sth to avoid duplicated where....
	def clause : Parser[(String, Any)] = 
		where | groupby | join | window
		
  
	def select : Parser[(String,List[SelectItem])] =
		"select"~>repsep(selectItem,",") ^^
		{s  => ("select", s)}

	def from : Parser[(String,Identifier)] =
		"from"~>identifier ^^
		{n => ("from",n)}

	def where : Parser[(String, Any)] = 
		"where"~>cond ^^
		{c => ("where",c)}

	def groupby : Parser[(String, List[Identifier])] =
		"group by"~>repsep(identifier,",") ^^
		{i => ("groupby",i)}


  def join : Parser[(String,JoinStatement)] =
    ("inner" | "left" | "right" | "full")~"join"~identifier~"on"~repsep(joincond, "and") ^^
      {case jtype~"join"~tab~"on"~onCol => ("join",new JoinStatement(jtype, tab, onCol)) }

  def window : Parser[(String, WindowProperty)] =
    "window"~>integer~integer ^^
      {case wd~sd => ("window",new WindowProperty(wd.i,sd.i))}

  def joincond : Parser[(ColOfTable,ColOfTable)] =
    identifier~"."~identifier~"="~identifier~"."~identifier ^^
      {case a~"."~b~"="~x~"."~y => (new ColOfTable(a,b), new ColOfTable(x,y))}

	def cond : Parser[Condition] =
		expr~("="|"<"|">"|"<="|">="|"<>")~expr ^^
		{case n~o~n2 => new Condition(n,o,n2)}

  def selectItem : Parser[SelectItem] =
    (identifier~"("~identifier~"."~identifier~") as"~identifier |
      identifier~"("~identifier~") as"~identifier  |
      identifier~"."~identifier~"as"~identifier  |
      identifier~"as"~identifier |
      identifier) ^^
      {
        case f~"("~t~"."~c~") as"~a => new SelectItem(t.asInstanceOf[Identifier],c.asInstanceOf[Identifier],f.asInstanceOf[Identifier],a.asInstanceOf[Identifier])
        case f~"("~c~") as"~a => new SelectItem(null,c.asInstanceOf[Identifier],f.asInstanceOf[Identifier],a.asInstanceOf[Identifier])
        case t~"."~c~"as"~n => new SelectItem(t.asInstanceOf[Identifier],c.asInstanceOf[Identifier],null,n.asInstanceOf[Identifier])
        case c~"as"~n => new SelectItem(null,c.asInstanceOf[Identifier],null,n.asInstanceOf[Identifier])
        case c => new SelectItem(null,c.asInstanceOf[Identifier],null,null)
      }



	def expr : Parser[Evalable] =
    float | integer | stringValue | identifier

//	def expr2 : Parser[List[Any]] = 
//		("+"|"-")~term ^^
//		{case s~f => List(s,f)}

//	def term : Parser[List[Any]] = 
//		factor~rep(term2) ^^
//		{case x~y => x::y.flatMap(a => a)}
//
//	def term2 : Parser[List[Any]] =
//		("*"|"/")~factor ^^
//		{case s~f => List(s,f)}
		
//	def factor : Parser[Evalable] =
//		float | "("~>expr<~")"

	def float : Parser[Float] =
    floatingPointNumber ^^
    {case f => new Float(f)}

  def integer : Parser[Integer] =
    """[0-9]+""".r ^^
      {case i => new Integer(i)}

  def stringValue : Parser[StringValue] = """"[.]*"""".r ^^ (a => new StringValue(a.substring(1).substring(0,a.length-2)))

	def identifier : Parser[Identifier] = """[a-zA-Z]+""".r ^^
		(a => new Identifier(a))

  def delimiter : Parser[String] = """"[^"]+"""".r ^^
    (a => a.replace("\"",""))

  def dataType : Parser[String] = "int" | "double" | "string"

  def iAddress : Parser[String] = """[^\s:]+""" .r

  def parseFile(file : String)  : List[Any] =  {
    val lines = Source.fromFile(file).getLines().toList
    lines.foreach(l=>println(parseAll(line, l.stripLineEnd)))
    lines.map(l => parseAll(line, l.stripLineEnd).get)
  }
}





