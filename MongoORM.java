import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoORM {
	/**
	 * use map<Object,ObjectId> to track whether we have pickled the object to
	 * avoid cycles references to objects with @MongoCollection annot always
	 * convert to ObjectId
	 */
	protected Map<Object, ObjectId> pickled = new HashMap<Object, ObjectId>();

	/** track objects as we load from mongo; don't reload, reuse if we can */
	protected Map<ObjectId, Object> depickled = new HashMap<ObjectId, Object>();

	protected DB db;

	public MongoORM(DB db) {
		this.db = db;
	}

	public DBCollection getCollection(Class<?> clazz) throws Exception {

		Annotation collectionAnnot = clazz.getAnnotation(MongoCollection.class);

		if (collectionAnnot == null) {
			return null;
		}

		Method collValueMethod = MongoCollection.class.getMethod("value");
		String collName = (String) collValueMethod.invoke(collectionAnnot);

		if (collName == null) {
			return null;
		}

		if (collName.equals("")) {
			collName = clazz.getName();
		}

		DBCollection collection = db.getCollection(collName);

		// returns null if there is none
		return collection;
	}

	public String getFieldName(Field f) throws Exception {
		Annotation fieldAnnot = f.getAnnotation(MongoField.class);
		if (fieldAnnot == null) {
			return null;
		}
		String fieldName = f.getName();
		// fieldName = yearlySalary, manager, projects

		Method fieldValueMethod = MongoField.class.getMethod("value");
		String nickname = (String) fieldValueMethod.invoke(fieldAnnot);
		if (!nickname.equals("")) {
			fieldName = nickname;
		}

		return fieldName;
	}

	public Collection<Object> populateDataStruct(Collection<Object> struct,
			Object data) throws Exception {

		for (Object o : (Collection<?>) data) {
			
//			System.out.println(o + "\t" + o.getClass().getName());
			
			Class<?> itemClass = o.getClass();
			
			if(itemClass == String.class) {
				
				struct.add(o);
				
			} else {
				String itemType = (String) ((BasicDBObject) o).get("type");
				ObjectId itemData = (ObjectId) ((BasicDBObject) o).get("data");

				itemClass = Class.forName(itemType);

				DBCollection coll = getCollection(itemClass);

				if (coll == null) {
					continue;
				}

				BasicDBObject query = new BasicDBObject();
				query.put("_id", itemData);

				DBObject itemObj = coll.findOne(query);

				if (itemObj == null) {
					continue;
				}

				Object obj = getObject(itemClass, itemObj);
				struct.add(obj);
			}
			
		}

		return struct;
	}

	public <T> Object getObject(Class<?> clazz, DBObject dbObj)
			throws Exception {

		// System.out.println("Class: " + clazz);

		T realObj = (T) clazz.newInstance();

		if (dbObj == null) {
			return null;
		}

		ObjectId id = (ObjectId) dbObj.get("_id");

		Object depickle = depickled.get(id);
		if (depickle != null) {
			return depickle;
		}

		depickled.put(id, realObj);

		List<Field> fields = getApplicableFields(clazz);

		for (Field f : fields) {

			String fieldName = getFieldName(f);

			if (fieldName == null) {
				continue;
			}

			Object value = dbObj.get(fieldName);

			if (value == null) {
				continue;
			}

			// checks if field is another Object
			if (value instanceof BasicDBObject) {

				Object data = ((BasicDBObject) value).get("data");
				String type = (String) ((BasicDBObject) value).get("type");

				Class<?> c = f.getType();

				// handle java collections
				if (c == List.class) {

					Class<?> listClass = Class.forName(type);
					List<Object> list = (List<Object>) listClass.newInstance();

					list = (List<Object>) populateDataStruct(list, data);

					f.set(realObj, list);

				} else if (c == Set.class) {

					Class<?> setClass = Class.forName(type);
					Set<Object> set = (Set<Object>) setClass.newInstance();

					set = (Set<Object>) populateDataStruct(set, data);

					f.set(realObj, set);

				} else if (c == Queue.class) {

					Class<?> queueClass = Class.forName(type);
					Queue<Object> queue = (Queue<Object>) queueClass
							.newInstance();

					queue = (Queue<Object>) populateDataStruct(queue, data);

					f.set(realObj, queue);

				} else {

					Class<?> itemClass = Class.forName(type);

					DBCollection coll = getCollection(itemClass);

					if (coll == null) {
						continue;
					}

					BasicDBObject query = new BasicDBObject();
					query.put("_id", data);

					DBObject itemObj = coll.findOne(query);

					if (itemObj == null) {
						continue;
					}

					Object obj = getObject(itemClass, itemObj);

					f.set(realObj, obj);
				}

			} else {
				// value is String or primitive type
				f.set(realObj, value);
			}

		}

		return realObj;

	}

	public void save(Object o) throws Exception {
		saveRec(o);
	}

	public List<Field> getApplicableFields(Class<?> clazz) throws Exception {

		List<Field> fields = new ArrayList<Field>();

		while (clazz != Object.class) {

			Annotation collectionAnnot = clazz
					.getAnnotation(MongoCollection.class);
			Method collValueMethod = MongoCollection.class.getMethod("value");
			String collName = (String) collValueMethod.invoke(collectionAnnot);

			// if an annotation on the collection exists
			if (collName != null) {
				Field[] fieldList = clazz.getDeclaredFields();
				for (Field f : fieldList) {
					fields.add(f);
				}
			}

			clazz = clazz.getSuperclass();
		}

		return fields;
	}

	public ObjectId saveRec(Object o) throws Exception {

		ObjectId pickle = pickled.get(o);

		if (pickle != null) {
			return pickle;
		}

		BasicDBObject dummy = new BasicDBObject();

		Class<?> clazz = o.getClass();

//		System.out.println("Object: " + clazz.getSimpleName() + "\t" + o);

		Annotation collectionAnnot = clazz.getAnnotation(MongoCollection.class);
		Method collValueMethod = MongoCollection.class.getMethod("value");

//		System.out.println(collValueMethod + "\t" + collectionAnnot);
		if (collectionAnnot == null) {
			return null;
		}

		String collName = (String) collValueMethod.invoke(collectionAnnot);
		// collName = empl

		if (collName.equals("")) {
			collName = clazz.getName();
		}

		DBCollection collection = db.getCollection(collName);
		collection.insert(dummy);
		ObjectId objID = (ObjectId) dummy.get("_id");

		pickled.put(o, objID);

		Method fieldValueMethod = MongoField.class.getMethod("value");

		List<Field> fields = getApplicableFields(clazz);

		BasicDBObject realObj = new BasicDBObject();

		for (Field f : fields) {

//			System.out.println("field: " + f.getName());

			Annotation fieldAnnot = f.getAnnotation(MongoField.class);

			if (fieldAnnot == null) {
				continue;
			}

			String fieldName = f.getName();
			// fieldName = yearlySalary, manager, projects

			String nickname = (String) fieldValueMethod.invoke(fieldAnnot);
			if (!nickname.equals("")) {
				fieldName = nickname;
			}
			// nickname = salary

			Object value = f.get(o);

//			System.out.println("value: " + value);

			if (value == null) {
				continue;
			}

			if (value instanceof Map) {
				System.out.println("ORM does not support Maps.");
				
			} else if (value instanceof Collection) {

//				System.out.println("COLLECTION");

				// "projects" : {"type" : "java.util.ArrayList",
				// "data" : [ {"type" : "Employee", "obj_id" :
				// ObjectId("1234")}, ] }

				// [ {"type" : "Employee", "obj_id" : ObjectId("1234")}, ]
				BasicDBList list = new BasicDBList();

				Collection<Object> collec = (Collection<Object>) value;
//				System.out.println("coll type " + value.getClass().getName());

				for (Object x : collec) {
					collectionAnnot = x.getClass().getAnnotation(
							MongoCollection.class);

					if (collectionAnnot != null) {
						ObjectId id = saveRec(x);

						// { "type" : "Employee", "obj_id" : ObjectId("1234") }
						BasicDBObject classObj = new BasicDBObject();

						// store JavaCollection type data
						String type = x.getClass().getName();
						classObj.put("type", type);
						classObj.put("data", id);

						list.add(classObj);
					} else {
						list.add(x);
					}

				}

				// {"type" : "java.util.ArrayList", "data" : [ {}, {} ] }
				BasicDBObject listObj = new BasicDBObject();

				// store JavaCollection type data
				String type = value.getClass().getName();
				listObj.put("type", type);
				listObj.put("data", list);

				realObj.put(fieldName, listObj);

			} else if (value instanceof Object) {

				Class<? extends Object> clazzs = value.getClass();
				Annotation collAnnot = clazzs
						.getAnnotation(MongoCollection.class);

				// if the value is a Mongo Collection (e.g. Manager)
				if (collAnnot != null) {

//					System.out.println("OBJECT");

					// store Object type data
					String type = value.getClass().getName();

					ObjectId id = saveRec(value);

					if (id != null) {
						// { "type" : "Employee", "obj_id" : ObjectId("1234") }
						BasicDBObject listObj = new BasicDBObject();

						listObj.put("type", type);
						listObj.put("data", id);

						realObj.put(fieldName, listObj);
					}

				} else {
//					System.out.println("STRING");
					realObj.put(fieldName, value);
				}

			} else {
//				System.out.println("PRIMITIVE");
				realObj.put(fieldName, value);
			}

		}

		collection.update(dummy, realObj);

		return objID;

	}

	public <T> List<T> loadAll(Class<T> clazz) throws Exception {

		// System.out.println(clazz);

		List<T> list = new ArrayList<T>();

		DBCollection coll = getCollection(clazz);

		DBCursor cursor = coll.find();

		while (cursor.hasNext()) {
			DBObject curr = cursor.next();
			// System.out.println("============ " + curr);
			T obj = (T) getObject(clazz, curr);
			list.add(obj);
		}

		return list;

	}

	public void printPickled() {
		for (Object o : pickled.keySet()) {
			System.out.println("object: " + o + "\tobject id: "
					+ pickled.get(o));
		}
	}

	public void printDepickled() {
		for (ObjectId i : depickled.keySet()) {
			System.out.println("object id: " + i + "\tobject: "
					+ depickled.get(i));
		}

	}

}