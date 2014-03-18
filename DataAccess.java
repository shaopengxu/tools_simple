package filedb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 一次加载策略，加载后不再从文件读取
 * 
 * @author 37wan
 * 
 */
public class DataAccess {

	private static DataAccess dataAccess = new DataAccess();

	private String topPath = "data";
	public static String slash = "/";
	public static int mapNumber = 1000;
	public static String underline = "_";
	public static String fileSuffix = ".obj";
	public static Map<String, Object> fileLockMap = new ConcurrentHashMap<String, Object>();

	public static DataAccess getInstance() {
		return dataAccess;
	}

	public synchronized Object getFileLockObject(String className, int id) {
		String s = className + id;
		Object obj = fileLockMap.get(s);
		if (obj == null) {
			obj = new Object();
			fileLockMap.put(s, obj);
		}
		return obj;
	}

	public void checkDirExist(String className, int id) {
		StringBuilder fileNameSb = new StringBuilder();
		fileNameSb.append(topPath);
		File file = new File(fileNameSb.toString());
		if (!file.exists()) {
			file.mkdir();
		}
		fileNameSb.append(slash).append(className);
		file = new File(fileNameSb.toString());
		if (!file.exists()) {
			file.mkdir();
		}
		fileNameSb.append(slash).append(id / mapNumber);
		file = new File(fileNameSb.toString());
		if (!file.exists()) {
			file.mkdir();
		}
	}

	public String getObjectFilePath(String className, int id) {
		return new StringBuilder().append(topPath).append(slash)
				.append(className).append(slash).append(id / mapNumber)
				.toString();
	}

	public String getObjectFileName(String className, int id) {
		return new StringBuilder().append(topPath).append(slash)
				.append(className).append(slash).append(id / mapNumber)
				.append(slash).append(className).append(underline).append(id)
				.append(fileSuffix).toString();
	}

	public void writeObject(Object o, int id) {
		String fileName = getObjectFileName(o.getClass().getSimpleName(), id);
		checkDirExist(o.getClass().getSimpleName(), id);
		try {
			// TODO 可以实现读写锁
			Object lock = getFileLockObject(o.getClass().getSimpleName(), id);
			synchronized (lock) {
				ObjectOutputStream out = new ObjectOutputStream(
						new FileOutputStream(fileName));
				out.writeObject(o);
				out.flush();
				out.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Object readObject(Class clazz, int id) {
		String fileName = getObjectFileName(clazz.getSimpleName(), id);
		checkDirExist(clazz.getSimpleName(), id);
		try {
			Object lock = getFileLockObject(clazz.getSimpleName(), id);
			synchronized (lock) {
				ObjectInputStream in = new ObjectInputStream(
						new FileInputStream(fileName));
				Object object = in.readObject();
				in.close();
				return object;
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		TestObject to = new TestObject();
		DataAccess da = getInstance();
		da.writeObject(to, 1);
		to = (TestObject) da.readObject(TestObject.class, 1);
		System.out.println(to.i);
	}

}

class TestObject implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int i = 3;
	int j = 4;
}
