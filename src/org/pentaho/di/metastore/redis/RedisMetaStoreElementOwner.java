package org.pentaho.di.metastore.redis;


import org.pentaho.metastore.api.security.IMetaStoreElementOwner;
import org.pentaho.metastore.api.security.MetaStoreElementOwnerType;

public class RedisMetaStoreElementOwner implements IMetaStoreElementOwner {

  private String name;
  private MetaStoreElementOwnerType type;

  public RedisMetaStoreElementOwner(String name, MetaStoreElementOwnerType type) {
    super();
    this.name = name;
    this.type = type;
  }
  
  public RedisMetaStoreElementOwner(IMetaStoreElementOwner owner) {
    this.name = owner.getName();
    this.type = owner.getOwnerType();
  }
  
  @Override
  public String getName() {
    return name;
  }

  @Override
  public MetaStoreElementOwnerType getOwnerType() {
    return type;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setOwnerType(MetaStoreElementOwnerType type) {
    this.type = type;
  }

}
