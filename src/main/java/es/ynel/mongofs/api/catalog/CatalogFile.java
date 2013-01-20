package es.ynel.mongofs.api.catalog;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.bson.types.ObjectId;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

public class CatalogFile
{
	private GridFSDBFile file;
	private GridFS gfs;
	private boolean isLoaded = false;
	private String path;
	
	public CatalogFile(GridFS gfs, GridFSDBFile file, String path)
	{
		this.gfs = gfs;
		this.file = file;
		this.path = path;
	}
	
	public String getPath()
	{
		return this.path;
	}
	
	public long getLength()
	{
		return this.file.getLength();
	}
	
	public String getContentType()
	{
		return this.file.getContentType();
	}
	
	public String getFilename()
	{
		return this.file.getFilename();
	}
	
	public String getMD5()
	{
		return this.file.getMD5();
	}
	
	public Date getUploadDate()
	{
		return this.file.getUploadDate();
	}
	
	public InputStream getInputStream()
	{
		if (!isLoaded)
		{
			this.file = gfs.findOne((ObjectId) this.file.getId());
		}
		
		return this.file.getInputStream();
	}
	
	public void update()
	{
		this.file.save();
	}
	
	public void delete()
	{
		this.gfs.remove((ObjectId) this.file.getId());
	}
	
	public void download(File file) throws IOException
	{
		if (!isLoaded)
		{
			this.file = gfs.findOne((ObjectId) this.file.getId());
		}
		
		this.file.writeTo(file);
	}
	
	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("CatalogFile ");
		builder.append("{filename: \"" + getFilename() + "\"");
		builder.append(", ");
		builder.append("length: " + getLength() + "}");
		return builder.toString();
	}
}
