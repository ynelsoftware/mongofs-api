package es.ynel.mongofs.api.repository;

import java.util.List;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import es.ynel.mongofs.api.config.Config;
import es.ynel.mongofs.api.document.CatalogData;


public class CatalogDataAdvancedRepository
{
	public List<CatalogData> getChildrenCatalogs(String path)
	{
		MongoTemplate mongoTemplate = Config.getBean(MongoTemplate.class);
		
		Query query = new Query(Criteria.where("path").regex("^" + path +"/(?=(.*/){0})(?!(.*/){1,}).*"));
		return mongoTemplate.find(query, CatalogData.class);
	}
	
	public List<CatalogData> getRecursiveChildrenCatalogs(String path)
	{
		MongoTemplate mongoTemplate = Config.getBean(MongoTemplate.class);
		
		Query query = new Query(Criteria.where("path").regex("(^" + (path.equals("/") ? "" : path) + "/|" + path + "$)"));
		return mongoTemplate.find(query, CatalogData.class);
	}
}
