package es.ynel.mongofs.api.catalog;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import es.ynel.mongofs.api.config.Config;
import es.ynel.mongofs.api.mongodb.FSConnection;


public class Catalog
{
	private String path;
	private String schema;
	private GridFS gfs;
	private List<Catalog> children;
	
	public Catalog(String path, String schema)
	{
		this.path = path;
		this.schema = schema;
		gfs = new GridFS(Config.getBean(DB.class), schema);
	}
	
	public List<CatalogFile> getFiles()
	{
		return this.getFiles(null);
	}
	
	public List<CatalogFile> getFiles(String filename)
	{
		if (filename != null)
		{
			List<GridFSDBFile> files = gfs.find(filename);
			if (files == null || files.size() == 0)
			{
				return Collections.EMPTY_LIST;
			}
			
			List<CatalogFile> catalogFiles = new ArrayList<CatalogFile>();
			for (GridFSDBFile file : files)
			{
				catalogFiles.add(new CatalogFile(gfs, file, this.path));
			}
			
			return catalogFiles;
		}
		
		DBCursor cursor = gfs.getFileList();
		if (cursor.size() == 0)
		{
			return Collections.EMPTY_LIST;
		}
		
		List<CatalogFile> catalogFiles = new ArrayList<CatalogFile>();
		List<DBObject> dbFiles = cursor.toArray();
		for (DBObject file : dbFiles)
		{
			catalogFiles.add(new CatalogFile(gfs, (GridFSDBFile) file, this.path));
		}
		
		return catalogFiles;
	}
	
	public CatalogFile addFile(String filename, InputStream is) throws IOException
	{
		GridFSInputFile gfsFile = gfs.createFile(is, true);
		gfsFile.setFilename(filename);
		gfsFile.save();
		GridFSDBFile gfdbf = gfs.findOne((ObjectId) gfsFile.getId());
		return new CatalogFile(gfs, gfdbf, filename);
	}
	
	public void addFile(String filename, File file) throws IOException
	{
		GridFSInputFile gfsFile = gfs.createFile(file);
		gfsFile.setFilename(filename);
		gfsFile.save();
	}
	
	public List<Catalog> getChildrenCatalogs() throws Exception
	{
		this.children = FSConnection.getInstance().getCatalogs(this.path);
		return this.children;
	}
	
	public Object getCatalogChild(int index) throws Exception
	{
		if (index >= this.getChildrenCatalogs().size())
		{
			return this.getFiles().get(index - this.getChildrenCatalogs().size());
		}
		else
		{
			return this.getChildrenCatalogs().get(index);
		}
	}
	
	public int getChildrenCount() throws Exception
	{
		return this.getChildrenCount(true);
	}
	
	public int getChildrenCount(boolean countFiles) throws Exception
	{
		return this.getChildrenCatalogs().size() + (countFiles ? this.getFiles().size() : 0);
	}
	
	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("Catalog ");
		builder.append("{path: \"" + path + "\"");
		builder.append(", ");
		builder.append("schema: \"" + schema + "\"}");
		return builder.toString();
	}

	public String getFolderName()
	{
		if (path.equals("/"))
		{
			return path;
		}
		
		return path.substring(path.lastIndexOf("/") + 1);
	}

	public String getPath()
	{
		return this.path;
	}
}
