package org.pentaho.di.metastore.redis;

public class RedisMetaStoreUtils {
  
  public static String DEFAULT_REDIS_PORT_STRING = "6379";
  
  /** Constant value for the delimiter between keyspace values */
  public static String KEY_SEPARATOR = "::";
  
  /** Constant value for the field name corresponding to an element's or attribute's value */
  public static String ATTR_VALUE_FIELD_NAME = "_value";
  
  /** Constant value for the field name corresponding to an element's or attribute's child attributes */
  public static String CHILDREN_FIELD_NAME = "children";
  
  /** Constant value for the field name corresponding to a namespace's collection of element types */
  public static String ELEMENT_TYPES_KEY_NAME = "elementTypes";
  
  /** Constant value for the field name corresponding to a element type's collection of elements */
  public static String ELEMENTS_KEY_NAME = "elements";
  
  public static String getKeyForElementType(String namespace, String elementTypeId) {
    StringBuffer hashKey = new StringBuffer(namespace);
    hashKey.append(KEY_SEPARATOR);
    hashKey.append(elementTypeId);
    
    return hashKey.toString();
  }

  public static String getKeyForElementTypes(String namespace) {
    StringBuffer hashKey = new StringBuffer(namespace);
    hashKey.append(KEY_SEPARATOR);
    hashKey.append(ELEMENT_TYPES_KEY_NAME);
    
    return hashKey.toString();
  }
  
  public static String getKeyForElement(String namespace, RedisMetaStoreElementType elementType, RedisMetaStoreElement element) {
    StringBuffer hashKey = new StringBuffer(getKeyForElementType(namespace, elementType.getId()));
    hashKey.append(KEY_SEPARATOR);
    hashKey.append(element.getId());
    
    return hashKey.toString();
  }
  
  public static String getKeyForElement(String namespace, RedisMetaStoreElement element) {
    return getKeyForElement(namespace, (RedisMetaStoreElementType)element.getElementType(), element);
  }
  
  public static String getKeyForElements(String namespace, RedisMetaStoreElementType elementType) {
    StringBuffer hashKey = new StringBuffer(getKeyForElementType(namespace, elementType.getId()));
    hashKey.append(KEY_SEPARATOR);
    hashKey.append(ELEMENTS_KEY_NAME);
    
    return hashKey.toString();
  }
  
  public static String addKeyForAttribute(String parentHashKey, RedisMetaStoreAttribute attr) {
    StringBuffer hashKey = new StringBuffer(parentHashKey);
    hashKey.append(KEY_SEPARATOR);
    hashKey.append(attr.getId());
    
    return hashKey.toString();
  }
  
  public static String getKeyForChildren(String parentHashKey) {
    StringBuffer hashKey = new StringBuffer(parentHashKey);
    hashKey.append(KEY_SEPARATOR);
    hashKey.append(CHILDREN_FIELD_NAME);
    
    return hashKey.toString();
  }
}
