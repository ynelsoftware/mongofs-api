package es.ynel.mongofs.api.repository;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import es.ynel.mongofs.api.document.CatalogData;

public interface CatalogDataRepository extends MongoRepository<CatalogData, ObjectId>
{
	public CatalogData findByPath(String path);
}
