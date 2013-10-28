package spark.streaming.examples

import spark.RDD
import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 10/16/13
 * Time: 9:15 PM
 * To change this template use File | Settings | File Templates.
 */
class OperatorGraph(_parentCtx : SqlSparkStreamingContext) {
  val parentCtx = _parentCtx
  val outputOperators = scala.collection.mutable.ArrayBuffer[OutputOperator]()
  val whereOperators = scala.collection.mutable.ArrayBuffer[WhereOperator]()
  val allOperators = scala.collection.mutable.ArrayBuffer[Operator]()
  val whereOperatorSets = ArrayBuffer[WhereOperatorSet]()
  val InnerJoinOperatorSets = ArrayBuffer[InnerJoinOperatorSet]()



  def addOperator(operator : Operator){
    allOperators += operator
    if(operator.isInstanceOf[OutputOperator])
      outputOperators += operator.asInstanceOf[OutputOperator]
    if(operator.isInstanceOf[WhereOperator])
      whereOperators += operator.asInstanceOf[WhereOperator]

  }

  def groupPredicate(){
    val visited = scala.collection.mutable.HashSet[Operator]()
    def visit(op : Operator, set : Option[WhereOperatorSet]){
      println("calling visit" + op)
      if(visited(op))
        return
      visited += op


      if(op.isInstanceOf[WhereOperator] && op.childOperators.size < 2) {
        if(set.isEmpty){
          val newSet = new WhereOperatorSet(parentCtx)
          newSet.addWhereOperator(op.asInstanceOf[WhereOperator])
          whereOperatorSets += newSet
          op.childOperators.foreach(c => c.replaceParent(op,newSet))
          op.parentOperators.foreach(p => visit(p,Some(newSet)))
        }else{
          set.get.addWhereOperator(op.asInstanceOf[WhereOperator])
          op.parentOperators.foreach(p => visit(p,set))
        }
      }else{
        if(!set.isEmpty){
          set.get.setParent(op)
        }
        op.parentOperators.foreach(p => visit(p, None))
      }
    }
    outputOperators.foreach(op => visit(op, None))
    println("finish")
  }

  def groupInnerJoin(){
    val visited = scala.collection.mutable.HashSet[Operator]()
    def visit(op : Operator, set : Option[InnerJoinOperatorSet]){
      if(visited(op))
        return
      visited += op

      if(op.isInstanceOf[InnerJoinOperator] && op.childOperators.size < 2){
        if(set.isEmpty){
          val newSet = new InnerJoinOperatorSet(parentCtx)
          newSet.addInnerJoinOperator(op.asInstanceOf[InnerJoinOperator])
          InnerJoinOperatorSets += newSet
          op.childOperators.foreach(c => c.replaceParent(op, newSet))
          op.parentOperators.foreach(p => visit(p, Some(newSet)))
        }else{
          set.get.addInnerJoinOperator(op.asInstanceOf[InnerJoinOperator])
          op.parentOperators.foreach(p => visit(p,set))
        }
      }else{
        if(!set.isEmpty){
          set.get.addParent(op)
        }
        op.parentOperators.foreach(p => visit(p, None))
      }
    }
    outputOperators.foreach(op => visit(op, None))
  }


  def pushAllPredicates = whereOperators.foreach(w => pushPredicate(w))

  def pushPredicate(operator : WhereOperator){

    var parentToReplaceWhenBinary : Operator = null

    def findPushTo(pushTo : Operator) : Operator = {
      val p =
        if(pushTo.isInstanceOf[UnaryOperator])
          pushTo.parentOperators.head
        else if(pushTo.isInstanceOf[BinaryOperator])
          if(pushTo.isInstanceOf[InnerJoinOperator]){
            parentToReplaceWhenBinary =
              if(operator.getWhereColumnId.subsetOf(pushTo.asInstanceOf[InnerJoinOperator].parentOperators(0).outputSchema.getLocalIdFromGlobalId.keySet))
                pushTo.parentOperators(0)
              else if(operator.getWhereColumnId.subsetOf(pushTo.asInstanceOf[InnerJoinOperator].parentOperators(1).outputSchema.getLocalIdFromGlobalId.keySet))
                pushTo.parentOperators(1)
              else
                throw new Exception()
            parentToReplaceWhenBinary
          }

      if(p.asInstanceOf[Operator].getChildOperators.size > 1)
        return pushTo
      p match{
        case p:WhereOperator => findPushTo(p)
        case p:SelectOperator => findPushTo(p)
        case p:GroupByOperator => {
          val gb = p.asInstanceOf[GroupByOperator]
          if(operator.getWhereColumnId.subsetOf(gb.getKeyColumnsArr.toSet)){
            findPushTo(p)
          }else{
            return pushTo
          }
        }
        case p:ParseOperator => return pushTo
        case p:InnerJoinOperator => {
          if(operator.getWhereColumnId.subsetOf(p.asInstanceOf[InnerJoinOperator].parentOperators(0).outputSchema.getLocalIdFromGlobalId.keySet) ||
            operator.getWhereColumnId.subsetOf(p.asInstanceOf[InnerJoinOperator].parentOperators(1).outputSchema.getLocalIdFromGlobalId.keySet) ){
            findPushTo(p)
          }else{
            return pushTo
          }
        }
      }
    }

    var pushTo : Operator = findPushTo(operator)

    if(pushTo == operator)
      return

    operator.childOperators.foreach(op => op.replaceParent(operator, operator.parentOperators.head))

    val oldParent = pushTo match{
      case pushTo : UnaryOperator => pushTo.parentOperators.head
      case pushTo : BinaryOperator => parentToReplaceWhenBinary
    }

    pushTo.replaceParent(oldParent,operator)
    operator.setParent(oldParent)

  }




  override def toString() : String = {
    val buffer = new StringBuilder
    outputOperators.foreach(op => buffer.append(op.getFamilyTree(0)))
    buffer.toString()
  }

  def execute(func : RDD[IndexedSeq[Any]] => Unit, exec : Execution){
    outputOperators.foreach(op => func(op.execute(exec)))
  }
}


