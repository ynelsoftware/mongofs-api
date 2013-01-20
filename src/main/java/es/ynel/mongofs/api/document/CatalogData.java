package es.ynel.mongofs.api.document;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection="catalogData")
public class CatalogData
{
	@Id
	private ObjectId id;
	
	@Indexed
	private String path;
	
	@Indexed
	private String schema;
	
	public ObjectId getId()
	{
		return id;
	}
	
	public String getPath()
	{
		return path;
	}
	
	public String getSchema()
	{
		return schema;
	}
	
	public void setId(ObjectId id)
	{
		this.id = id;
	}
	
	public void setPath(String path)
	{
		this.path = path;
	}
	
	public void setSchema(String schema)
	{
		this.schema = schema;
	}
}
