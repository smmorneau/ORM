/* Author: Terence Parr */

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** annotations with this type are to be retained by the VM so they can be read
 *  reflectively at run-time
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface MongoCollection {

	String value() default "";

	

}
