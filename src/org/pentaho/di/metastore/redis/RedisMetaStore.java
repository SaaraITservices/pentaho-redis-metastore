package org.pentaho.di.metastore.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pentaho.di.core.Const;
import org.pentaho.metastore.api.BaseMetaStore;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreElementExistException;
import org.pentaho.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.pentaho.metastore.api.security.IMetaStoreElementOwner;
import org.pentaho.metastore.api.security.MetaStoreElementOwnerType;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisMetaStore extends BaseMetaStore implements IMetaStore {

  //TODO: Make the operations more scalable (SPOP instead of SMEMBERS, e.g.)  
  
  private Jedis jedis;
  
  private boolean createNamespaceIfNotExists = true;
  
  public RedisMetaStore() throws MetaStoreException {
    
    // Attempt to get metastore settings by property since none were provided to the constructor
    String redisHost = System.getProperty("pentaho.metastore.redis.host");
    if(null != redisHost) {
      int redisPort = Integer.parseInt(System.getProperty("pentaho.metastore.redis.port", RedisMetaStoreUtils.DEFAULT_REDIS_PORT_STRING));
      String redisTimeoutString = System.getProperty("pentaho.metastore.redis.timeout");
      if(null == redisTimeoutString) {
        jedis = new Jedis(redisHost, redisPort);
      }
      else {
        jedis = new Jedis(redisHost, redisPort, Integer.parseInt(redisTimeoutString));
      }
    }
    else {
      throw new MetaStoreException("Couldn't connect to Redis metastore, check your pentaho.metastore.redis.* properties");
    }
    
  }
  
  public RedisMetaStore(final String host) {
    jedis = new Jedis(host);
  }

  public RedisMetaStore(final String host, final int port) {
    jedis = new Jedis(host, port);
  }

  public RedisMetaStore(final String host, final int port, final int timeout) {
    jedis = new Jedis(host, port, timeout);
  }
  
  public boolean connect() throws MetaStoreException {
    if(null == jedis) throw new MetaStoreException("Couldn't connect to Redis metastore, no connection information provided");
    
    if(jedis.isConnected()) return true;
    
    jedis.connect();
    
    return jedis.isConnected();
  }
  
  public void disconnect() {
    if(null != jedis) {
      jedis.disconnect();
    }
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof XmlMetaStore)) {
      return false;
    }
    return ((RedisMetaStore) obj).getName().equalsIgnoreCase(getName());
  }
  
  @Override
  public void createElement(String namespace, IMetaStoreElementType elementType, IMetaStoreElement element) 
      throws MetaStoreException, MetaStoreElementExistException {
    
    if(!namespaceExists(namespace)) {
      if(createNamespaceIfNotExists) {
        createNamespace(namespace);
      }
      else {
        throw new MetaStoreException("Namespace "+namespace+" does not exist!");
      }
      
      IMetaStoreElementType existingElementType = getElementType(namespace, elementType.getName());
      if(null == existingElementType) {
        throw new MetaStoreException("Element type "+elementType.getName()+" does not exist in namespace "+namespace);
      }
      
      // Link element to element type in Redis (or throw exception if it already exists)
      String elementsKey = RedisMetaStoreUtils.getKeyForElements(namespace, (RedisMetaStoreElementType)elementType);
      if(jedis.sismember(elementsKey, element.getId())) {
        throw new MetaStoreElementExistException(
            getElements(namespace,elementType), 
            "Element id "+element.getId()+" of type "+elementType.getName()+" already exists in namespace "+namespace);
      }
      
      // Link element to type
      jedis.sadd(elementsKey, element.getId());
      
      // Create Redis hash for element (and child attributes if necessary)
      RedisMetaStoreElement redisElement = new RedisMetaStoreElement(element);
      redisElement.setElementType(elementType);
      save(namespace, redisElement);
    }
    
  }
  
  private void save(String namespace, RedisMetaStoreElement element) {
    String hashKey = RedisMetaStoreUtils.getKeyForElement(namespace, element);
    
    // Save element's attribute properties (value, children, etc.)
    save(namespace, (RedisMetaStoreAttribute)element, hashKey);
    
    // Add element to type's element set
    
  }
  
  private void save(String namespace, RedisMetaStoreAttribute attr, String attrKey) {
    
    // Set special "_value" field to hold the element's value
    jedis.hset(attrKey, RedisMetaStoreUtils.ATTR_VALUE_FIELD_NAME, attr.getValue().toString());
    
    List<IMetaStoreAttribute> children = attr.getChildren();
    
    if(null != children) {
      String childrenKey = RedisMetaStoreUtils.getKeyForChildren(attrKey);
      for (IMetaStoreAttribute child : children) {
        // Create a new hash for each child
        String attrHashKey = RedisMetaStoreUtils.addKeyForAttribute(attrKey, (RedisMetaStoreAttribute)child);
        jedis.hset(attrHashKey, RedisMetaStoreUtils.ATTR_VALUE_FIELD_NAME, child.getValue().toString());
        
        // Add child's hash id to parent's children hash
        jedis.hset(childrenKey, attr.getId(), attrHashKey);
      }
    }
  }

  @Override
  public void createElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException,
      MetaStoreElementTypeExistsException {
    List<IMetaStoreElementType> elementTypes = getElementTypes(namespace);
    
    if(null != elementTypes) {
      if(elementTypes.contains(elementType)) {
        throw new MetaStoreElementTypeExistsException(elementTypes, "Element type "+elementType.getName()+" already exists in namespace "+namespace);
      }
    }
    
    // Create element type by creating a hash for its properties and adding the type id to the namespace type set
    String elementTypeKey = RedisMetaStoreUtils.getKeyForElementType(namespace, elementType.getId());
    Transaction txaction = jedis.multi();
    txaction.hset(elementTypeKey, "id", elementType.getId());
    txaction.hset(elementTypeKey, "name", elementType.getName());
    txaction.hset(elementTypeKey, "namespace", elementType.getNamespace());
    txaction.hset(elementTypeKey, "metaStoreName", elementType.getMetaStoreName());
    txaction.hset(elementTypeKey, "description", elementType.getDescription());
    txaction.sadd(RedisMetaStoreUtils.getKeyForElementTypes(namespace), elementType.getId());
    txaction.exec();
  }

  @Override
  public void createNamespace(String namespace) throws MetaStoreException, MetaStoreNamespaceExistsException {
    if(jedis.sismember("namespaces", namespace)) {
      throw new MetaStoreNamespaceExistsException("Namespace already exists: "+namespace);
    }
    jedis.sadd("namespaces", namespace);    
  }

  @Override
  public void deleteElement(String namespace, IMetaStoreElementType elementType, String elementId) throws MetaStoreException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void deleteElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException,
      MetaStoreDependenciesExistsException {
    // Referential integrity is provided in a relational way; this means that any elements of types in the namespace
    // must be deleted first before the element type can be deleted.
    List<String> elementIds = getElementIds(namespace, elementType);
    if(!Const.isEmpty(elementIds)) {
      throw new MetaStoreDependenciesExistsException(
          elementIds, "Cannot delete element type "+elementType.getName()+" from namespace "+namespace+", elements of this type still exist! (referential integrity error)");
    }
    String elementTypeKey = RedisMetaStoreUtils.getKeyForElementType(namespace, elementType.getId());
    Transaction txaction = jedis.multi();
    txaction.del(elementTypeKey);
    txaction.srem(RedisMetaStoreUtils.getKeyForElementTypes(namespace), elementType.getId());
    txaction.exec();
  }

  @Override
  public void deleteNamespace(String namespace) throws MetaStoreException, MetaStoreDependenciesExistsException {
    // Referential integrity is provided in a relational way; this means that any elements of types in the namespace
    // must be deleted first, then the types must be deleted. The former will be checked by a deleteElementType() call,
    // so here we only need to see if the namespace has any associated types
    Set<String> elementTypes = jedis.smembers(RedisMetaStoreUtils.getKeyForElementTypes(namespace));
    if(elementTypes != null && elementTypes.size() > 0) {
      throw new MetaStoreDependenciesExistsException(
          new ArrayList<String>(elementTypes),
          "Cannot delete namespace "+namespace+", element types still exist! (referential integrity error)");
    }
    jedis.srem("namespaces", namespace);
    
  }

  @Override
  public IMetaStoreElement getElement(String namespace, IMetaStoreElementType elementType, String elementId) throws MetaStoreException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IMetaStoreElement getElementByName(String namespace, IMetaStoreElementType elementType, String elementName)
      throws MetaStoreException {
    // TODO
    return null;
  }

  @Override
  public List<String> getElementIds(String namespace, IMetaStoreElementType elementType) throws MetaStoreException {
    return new ArrayList<String>(jedis.smembers(RedisMetaStoreUtils.getKeyForElements(namespace,(RedisMetaStoreElementType)elementType)));
  }

  @Override
  public IMetaStoreElementType getElementType(String namespace, String elementTypeId) throws MetaStoreException {
    String elementTypeKey = RedisMetaStoreUtils.getKeyForElementType(namespace, elementTypeId);
    Map<String,String> elementTypeProperties = jedis.hgetAll(elementTypeKey);
    if(elementTypeProperties != null && !elementTypeProperties.isEmpty()) {
      RedisMetaStoreElementType elementType = new RedisMetaStoreElementType();
      elementType.setId(elementTypeProperties.get("id"));
      elementType.setName(elementTypeProperties.get("name"));
      elementType.setNamespace(elementTypeProperties.get("namespace"));
      elementType.setMetaStoreName(elementTypeProperties.get("metaStoreName"));
      elementType.setDescription(elementTypeProperties.get("description"));
      return elementType;
    }
    return null;
  }

  @Override
  public IMetaStoreElementType getElementTypeByName(String namespace, String elementTypeName) throws MetaStoreException {
    List<IMetaStoreElementType> elementTypes = getElementTypes(namespace);
    if(elementTypes != null) {
      for(IMetaStoreElementType elementType : elementTypes) {
        if(elementType.getName().equals(elementTypeName)) {
          return elementType;
        }
      }
    }
    return null;
  }

  @Override
  public List<String> getElementTypeIds(String namespace) throws MetaStoreException {
    /*ArrayList<String> elementTypeIds = null;
    List<IMetaStoreElementType> elementTypes = getElementTypes(namespace);
    if(elementTypes != null) {
      elementTypeIds = new ArrayList<String>(elementTypes.size());          
      for(IMetaStoreElementType elementType : elementTypes) {
        elementTypeIds.add(elementType.getId());
      }
    }*/
    return new ArrayList<String>(jedis.smembers(RedisMetaStoreUtils.getKeyForElementTypes(namespace)));
  }

  @Override
  public List<IMetaStoreElementType> getElementTypes(String namespace) throws MetaStoreException {
    List<IMetaStoreElementType> elementTypes = null;
    List<String> elementTypeIds = getElementTypeIds(namespace);
    if(elementTypeIds != null) {
      elementTypes = new ArrayList<IMetaStoreElementType>(elementTypeIds.size());
      for(String elementTypeId : elementTypeIds) {
        elementTypes.add(getElementType(namespace, elementTypeId));
      }
    }
    return elementTypes;
  }

  @Override
  public List<IMetaStoreElement> getElements(String namespace, IMetaStoreElementType elementType) throws MetaStoreException {
    List<IMetaStoreElement> elements = null;
    List<String> elementIds = getElementIds(namespace, elementType);
    if(elementIds != null) {
      elements = new ArrayList<IMetaStoreElement>(elementIds.size());
      for(String elementTypeId : elementIds) {
        elements.add(getElement(namespace, elementType, elementTypeId));
      }
    }
    return elements;
  }

  @Override
  public List<String> getNamespaces() throws MetaStoreException {
    return new ArrayList<String>(jedis.smembers("namespace"));
  }

  @Override
  public boolean namespaceExists(String namespace) throws MetaStoreException {
    return jedis.sismember("namespace", namespace);
  }

  @Override
  public IMetaStoreAttribute newAttribute(String id, Object value) throws MetaStoreException {
    return new RedisMetaStoreAttribute(id,value);
  }

  @Override
  public IMetaStoreElement newElement() throws MetaStoreException {
    return new RedisMetaStoreElement();
  }

  @Override
  public IMetaStoreElement newElement(IMetaStoreElementType elementType, String elementId, Object value) throws MetaStoreException {
    RedisMetaStoreElement element = new RedisMetaStoreElement();
    element.setElementType(elementType);
    element.setId(elementId);
    element.setValue(value);
    return element;
  }

  @Override
  public IMetaStoreElementOwner newElementOwner(String name, MetaStoreElementOwnerType elementOwnerType) throws MetaStoreException {
    return new RedisMetaStoreElementOwner(name, elementOwnerType);
  }

  @Override
  public IMetaStoreElementType newElementType(String namespace) throws MetaStoreException {
    return new RedisMetaStoreElementType(namespace);
  }

  @Override
  public void updateElement(String namespace, IMetaStoreElementType elementType, String elementId, IMetaStoreElement element)
      throws MetaStoreException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void updateElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException {
    // TODO Auto-generated method stub
    
  }

}
