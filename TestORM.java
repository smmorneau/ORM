import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class TestORM {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Mongo m = new Mongo();
		DB db = m.getDB("orm");
		db.dropDatabase();
		MongoORM orm = new MongoORM(db);

		/* SAVE */
		
		Project project1 = new Project();
		project1.name = "pp1";
		project1.ll = new LinkedList<String>();
		project1.ll.add("linked");
		project1.ll.add("list");
		
		orm.save(project1);
		
		Project project2 = new Project();
		project2.name = "pp2";
		project1.ll = new LinkedList<String>();
		project1.ll.add("asdf");
		project1.ll.add("ghjkl");
		
		Employee parrt = new Employee();
		parrt.name = "parrt";
		
		parrt.projects = new ArrayList<Project>();
		parrt.projects.add(project1);
		parrt.projects.add(project2);
		
		parrt.yearlySalary = (float) 100.5;
		
		parrt.projectsS = new HashSet<String>();
		parrt.projectsS.add("A");
		parrt.projectsS.add("B");

		orm.save(parrt);

		Manager steely = new Manager();
		steely.name = "steely";
		steely.yearlySalary = (float) 500;
		steely.parkingSpot = 3;
		
		steely.projects = new ArrayList<Project>();
		steely.projects.add(new Project("X"));
		steely.projects.add(new Project("Y"));
		
		steely.directReports = new ArrayList<Employee>();
		steely.directReports.add(parrt);
		steely.directReports.add(new Employee("shah"));

		steely.map = new HashMap<Project,Employee>();
		steely.map.put(project1, parrt);

		orm.save(steely);

		Employee tombu = new Employee();
		tombu.name = "tombu";
		tombu.yearlySalary = (float) 300;
		// tombu.manager = parrt;
		tombu.manager = steely;
		tombu.projects = new ArrayList<Project>();
		tombu.projectsQ = new LinkedList<Project>();

		tombu.projects.add(new Project("p1"));
		tombu.projects.add(new Project("p2"));

		tombu.projectsQ.add(new Project("p3"));
		tombu.projectsQ.add(new Project("p4"));

		orm.save(tombu);

		/* LOAD */
		
		// clear pickled and depickled
		orm.depickled = new HashMap<ObjectId, Object>();
		orm.pickled = new HashMap<Object, ObjectId>();

		List<Employee> empls = null;
		List<Manager> mngrs = null;
		List<Project> prjcts = null;

		mngrs = orm.loadAll(Manager.class);
		empls = orm.loadAll(Employee.class);
		prjcts = orm.loadAll(Project.class);

		// System.out.println();
		System.out.println("employees: " + empls);
		System.out.println("managers: " + mngrs);
		System.out.println("projects: " + prjcts);
	}

}
