package ch.epfl.dias.cs422.helpers

import java.lang.reflect.Constructor

object Construct {
  def create[T](c: Class[_ <: T], args: Any*): T = {
    c.getConstructors.find(
      constr => {
        constr.getAnnotatedReturnType.getType == c &&
        constr.getParameterCount == args.length &&
        constr.getParameterTypes.zip(args).forall {
          case (_, null) => true
          case (c, e) => c.isInstance(e)
        }
      }
    ).get.asInstanceOf[Constructor[_ <: T]].newInstance(args: _*)
  }
}
