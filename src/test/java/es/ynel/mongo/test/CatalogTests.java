package es.ynel.mongo.test;

import java.util.List;

import org.junit.Test;

import es.ynel.mongofs.api.catalog.Catalog;
import es.ynel.mongofs.api.mongodb.FSConnection;

public class CatalogTests
{
	@Test
	public void createTestPath() throws Exception
	{
		FSConnection mc = FSConnection.getInstance();
		String path = "/test";
		Catalog catalog = mc.getCatalog(path);
		if (catalog == null)
		{
			System.out.println("Creating catalog " + path);
			catalog = mc.createCatalog(path);
		}
		
		System.out.println(catalog);
	}
	
	@Test
	public void findChildrenFromPath() throws Exception
	{
		FSConnection mc = FSConnection.getInstance();
		String path = "/test";
		List<Catalog> catalogs = mc.getCatalogs(path);
		if (catalogs == null || catalogs.size() == 0)
		{
			System.out.println("No children found");
			return;
		}
		
		for (Catalog catalog : catalogs)
		{
			System.out.println(catalog);
		}
	}
}
