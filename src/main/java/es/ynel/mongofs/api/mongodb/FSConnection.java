package es.ynel.mongofs.api.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import es.ynel.mongofs.api.catalog.Catalog;
import es.ynel.mongofs.api.config.Config;
import es.ynel.mongofs.api.document.CatalogData;
import es.ynel.mongofs.api.repository.CatalogDataAdvancedRepository;
import es.ynel.mongofs.api.repository.CatalogDataRepository;

public class FSConnection
{
	private static FSConnection instance;
	private CatalogDataRepository cdr;
	private MongoTemplate mongoTemplate;
	private boolean checkedRoot;
	
	private Log log = LogFactory.getLog(getClass());
	
	private FSConnection() throws Exception
	{
		mongoTemplate = Config.getBean(MongoTemplate.class);
		cdr = Config.getBean(CatalogDataRepository.class);
	}
	
	/**
	 * Returns the connection to the database
	 * @return
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public static FSConnection getInstance() throws Exception
	{
		if (instance == null)
		{
			instance = new FSConnection();
		}
		
		return instance;
	}
	
	/**
	 * Get the catalog from database if exists
	 * @param path Catalog path
	 * @return Catalog
	 * @throws Exception
	 */
	public Catalog getCatalog(String path) throws Exception
	{
		if (!path.startsWith("/"))
		{
			throw new Exception("Path must starts with /");
		}
		
		this.checkRootPath();
		
		String schema = this.getCollectionNameFromPath(path);
		if (schema == null)
		{
			return null;
		}
		
		Catalog catalog = new Catalog(path, schema);
		return catalog;
	}
	
	/**
	 * Delete all catalogs from database except root Catalog
	 * @throws Exception
	 */
	public void deleteAllCatalogs() throws Exception
	{
		this.deleteCatalog("/");
	}
	
	/**
	 * Delete catalog with the given path from database
	 * @param path Catalog path to remove
	 * @throws Exception
	 */
	public void deleteCatalog(String path) throws Exception
	{
		List<Catalog> catalogs = this.getCatalogsChildren(path, true);
		
		for (Catalog catalog : catalogs)
		{
			if (catalog.getPath().equals("/"))
			{
				DBObject query = new BasicDBObject();
				query.put("_id", new BasicDBObject("$ne",""));
				mongoTemplate.getCollection("root").remove(query);
				continue;
			}
			
			String schema = this.getCollectionNameFromPath(catalog.getPath());
			mongoTemplate.dropCollection(schema);
			mongoTemplate.dropCollection(schema + ".chunks");
			mongoTemplate.dropCollection(schema + ".files");
			
			CatalogData catalogData = new CatalogData();
			catalogData.setSchema(schema);
			cdr.delete(catalogData);
		}
	}
	
	/**
	 * Create catalog with the given path recursively. If the catalog exists returns the Catalog
	 * @param path Catalog path
	 * @return Catalog with the given path
	 * @throws Exception
	 */
	public Catalog createCatalog(String path) throws Exception
	{
		if (!path.startsWith("/"))
		{
			throw new Exception("Path must starts with /");
		}
		
		this.checkRootPath();
		
		String schema = this.getCollectionNameFromPath(path);
		if (schema == null)
		{
			StringTokenizer tokenizer = new StringTokenizer(path, "/");
			
			String createPath = "";
			while (tokenizer.hasMoreTokens())
			{
				createPath += "/" + tokenizer.nextToken();
				this.createPath(createPath);
			}
			
			CatalogData newCatalog = cdr.findByPath(createPath);
			
			if (newCatalog == null)
			{
				throw new Exception("Catalog with path " + createPath + " doesn't exist");
			}
			
			Catalog catalog = new Catalog(createPath, newCatalog.getSchema());
			return catalog;
		}
		
		Catalog catalog = new Catalog(path, schema);
		return catalog;
	}
	
	/**
	 * Create a new catalog in the database with the given path
	 * @param path
	 * @throws Exception
	 */
	private void createPath(String path) throws Exception
	{
		CatalogData pathCatalog = cdr.findByPath(path);
		
		if (pathCatalog == null)
		{
			UUID uuid = UUID.randomUUID();

			if (mongoTemplate.collectionExists(uuid.toString()))
			{
				//TODO ???
				throw new Exception("UUID " + uuid.toString() + " already exists");
			}
			
			pathCatalog = new CatalogData();
			pathCatalog.setPath(path);
			pathCatalog.setSchema(uuid.toString());
			
			mongoTemplate.createCollection(uuid.toString());
			mongoTemplate.save(pathCatalog);
			
			pathCatalog = cdr.findByPath(path);
			log.debug("Created new catalog with path " + path);
		}
		else
		{
			if (!mongoTemplate.collectionExists(pathCatalog.getSchema()))
			{
				mongoTemplate.createCollection(pathCatalog.getSchema());
			}
		}
	}

	/**
	 * Checks if the root path is created
	 */
	private void checkRootPath()
	{
		if (checkedRoot)
		{
			return;
		}
		
		if (!mongoTemplate.collectionExists(CatalogData.class))
		{
			mongoTemplate.createCollection(CatalogData.class);
		}
		
		CatalogData rootCatalog = cdr.findByPath("/");
		if (rootCatalog == null)
		{
			log.debug("Creating Root path ");
			
			rootCatalog = new CatalogData();
			rootCatalog.setPath("/");
			rootCatalog.setSchema("root");
			cdr.save(rootCatalog);
			mongoTemplate.createCollection("root");
		}
		
		checkedRoot = true;
	}
	
	/**
	 * Returns the schema associated to the catalog path
	 * @param path Path to find the schema name
	 * @return Schema name associated
	 */
	private String getCollectionNameFromPath(String path)
	{
		CatalogData data = cdr.findByPath(path);
		if (data == null)
		{
			return null;
		}
		else
		{
			return data.getSchema();
		}
	}

	/**
	 * Returns all the catalogs in the database excluding the root catalog
	 * @return
	 */
	public List<Catalog> getCatalogs()
	{
		return this.getCatalogs("", false);
	}
	
	/**
	 * Returns all the catalogs in the database including the root catalog
	 * @return
	 */
	public List<Catalog> getCatalogsIncludingRoot()
	{
		return this.getCatalogs("", true);
	}
	
	/**
	 * Returns the catalogs which theirs parents is the given parent path
	 * @param parentPath Parent path to search
	 * @return
	 */
	public List<Catalog> getCatalogs(String parentPath)
	{
		return this.getCatalogs(parentPath, false);
	}
	
	/**
	 * Returns the catalogs which theirs parents is the given parent path
	 * @param parentPath Parent path to search
	 * @param includeRoot If true, includes root path in the list
	 * @return
	 */
	public List<Catalog> getCatalogs(String parentPath, boolean includeRoot)
	{
		this.checkRootPath();
		
//		query.put("path", new BasicDBObject("$regex", "^" + parentPath +"/(?=(.*/){0})(?!(.*/){1,}).*"));
//		if (parentPath.equals("") && !includeRoot)
//		{
//			query.put("schema", new BasicDBObject("$ne", "root"));
//		}
//		
		CatalogDataAdvancedRepository cdar = new CatalogDataAdvancedRepository();
		List<CatalogData> catalogDataList = cdar.getChildrenCatalogs(parentPath);
		if (catalogDataList.size() == 0)
		{
			return Collections.EMPTY_LIST;
		}
		
		List<Catalog> catalogs = new ArrayList<Catalog>();
		for (CatalogData catalogData : catalogDataList)
		{
			catalogs.add(new Catalog(catalogData.getPath(), catalogData.getSchema()));
		}
		
		return catalogs; 
	}
	
	/**
	 * Returns the catalogs which theirs parents is the given parent path and all their children
	 * @param parentPath Parent path to search
	 * @param includeRoot If true, includes root path in the list
	 * @return
	 */
	public List<Catalog> getCatalogsChildren(String parentPath, boolean includeRoot)
	{
		this.checkRootPath();
		
//		query.put("path", new BasicDBObject("$regex", "(^" + (parentPath.equals("/") ? "" : parentPath) + "/|" + parentPath + "$)"));
//		if (parentPath.equals("") && !includeRoot)
//		{
//			query.put("schema", new BasicDBObject("$ne", "root"));
//		}
		
		CatalogDataAdvancedRepository cdar = new CatalogDataAdvancedRepository();
		List<CatalogData> catalogDataList = cdar.getChildrenCatalogs(parentPath);
		if (catalogDataList.size() == 0)
		{
			return Collections.EMPTY_LIST;
		}
		
		List<Catalog> catalogs = new ArrayList<Catalog>();
		for (CatalogData catalogData : catalogDataList)
		{
			catalogs.add(new Catalog(catalogData.getPath(), catalogData.getSchema()));
		}
		
		return catalogs; 
	}
}
