package es.ynel.mongo.test;

import java.io.File;
import java.util.List;

import org.junit.Test;

import es.ynel.mongofs.api.catalog.Catalog;
import es.ynel.mongofs.api.catalog.CatalogFile;
import es.ynel.mongofs.api.mongodb.FSConnection;

public class FileTests
{

	@Test
	public void uploadFile() throws Exception
	{
		Catalog catalog = FSConnection.getInstance().createCatalog("/test");
		
		String newFileName = Long.toHexString(Double.doubleToLongBits(Math.random()));
		File imageFile = new File("/home/ynelsoftware/tmp/testimage.jpg");
		catalog.addFile(newFileName, imageFile);
		System.out.println(catalog.getFiles());
	}
	
	@Test
	public void showFiles() throws Exception
	{
		Catalog catalog = FSConnection.getInstance().createCatalog("/files/test");
		System.out.println(catalog.getFiles());
	}
	
	@Test
	public void downloadFirstFile() throws Exception
	{
		Catalog catalog = FSConnection.getInstance().createCatalog("/files/test");
		List<CatalogFile> catalogFiles = catalog.getFiles();
		if (catalogFiles.size() == 0)
		{
			System.out.println("There is no files");
		}
		
		catalogFiles.get(0).download(new File("/home/ynelsoftware/tmp/testimage"));
	}
}
