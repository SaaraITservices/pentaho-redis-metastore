package org.pentaho.di.metastore.redis;

import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

public class RedisMetaStoreElementType implements IMetaStoreElementType {

  private String namespace;
  private String id;
  private String name;
  private String description;
  private String metaStoreName;
  
  public RedisMetaStoreElementType() {
    this.namespace = null;
    this.id = null;
    this.name = null;
    this.description = null;
    this.metaStoreName = null;
  }
  
  public RedisMetaStoreElementType(String namespace) {
    this();
    this.namespace = namespace;
  }
  
  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getMetaStoreName() {
    return metaStoreName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public void save() throws MetaStoreException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public void setMetaStoreName(String metaStoreName) {
    this.metaStoreName = metaStoreName;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

}
