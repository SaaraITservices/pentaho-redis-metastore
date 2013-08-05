package org.pentaho.di.metastore.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.metastore.api.IMetaStoreAttribute;

public class RedisMetaStoreAttribute implements IMetaStoreAttribute {

  protected String id;
  protected Object value;

  protected Map<String, IMetaStoreAttribute> children;
  
  public RedisMetaStoreAttribute() {
    children = new HashMap<String, IMetaStoreAttribute>();
    this.id = null;
    this.value = null;
  }
  
  public RedisMetaStoreAttribute(String id, Object value) {
    this();
    this.id = id;
    this.value = value;
  }
  
  /**
   * Duplicate the element data into this structure.
   * @param element
   */
  public RedisMetaStoreAttribute(IMetaStoreAttribute element) {
    this();
    id = element.getId();
    value = element.getValue();
    for (IMetaStoreAttribute childElement : element.getChildren()) {
      addChild(new RedisMetaStoreAttribute(childElement));
    }
  }
  
  @Override
  public void addChild(IMetaStoreAttribute arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void clearChildren() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void deleteChild(String arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public IMetaStoreAttribute getChild(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<IMetaStoreAttribute> getChildren() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getValue() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setId(String arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setValue(Object arg0) {
    // TODO Auto-generated method stub
    
  }

}
