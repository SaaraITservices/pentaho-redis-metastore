package org.pentaho.di.metastore.redis;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.security.IMetaStoreElementOwner;
import org.pentaho.metastore.api.security.MetaStoreOwnerPermissions;

public class RedisMetaStoreElement extends RedisMetaStoreAttribute implements IMetaStoreElement {

  protected String name;

  protected IMetaStoreElementType elementType;
  
  protected RedisMetaStoreElementOwner owner;
  protected List<MetaStoreOwnerPermissions> ownerPermissionsList;
  
  /**
   * Default no-arg constructor
   */
  public RedisMetaStoreElement() {
    super();
    this.name = null;
    this.owner = null;
    this.ownerPermissionsList = new ArrayList<MetaStoreOwnerPermissions>();
  }
  
  /**
   * Duplicate the element data into this structure.
   * @param element
   */
  public RedisMetaStoreElement(IMetaStoreElement element) {
    super(element);
    this.name = element.getName();
    this.ownerPermissionsList = new ArrayList<MetaStoreOwnerPermissions>();
    if (element.getOwner()!=null) {
      this.owner = new RedisMetaStoreElementOwner(element.getOwner());
    }
    for (MetaStoreOwnerPermissions ownerPermissions : element.getOwnerPermissionsList()) {
      this.getOwnerPermissionsList().add( new MetaStoreOwnerPermissions(ownerPermissions.getOwner(), ownerPermissions.getPermissions()) );
    }
  }
  
  @Override
  public IMetaStoreElementType getElementType() {
    return elementType;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public IMetaStoreElementOwner getOwner() {
    return owner;
  }

  @Override
  public List<MetaStoreOwnerPermissions> getOwnerPermissionsList() {
    return ownerPermissionsList;
  }

  @Override
  public void setElementType(IMetaStoreElementType elementType) {
    this.elementType = elementType;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setOwner(IMetaStoreElementOwner owner) {
    this.owner = new RedisMetaStoreElementOwner(owner);
  }

  @Override
  public void setOwnerPermissionsList(List<MetaStoreOwnerPermissions> ownerPermissionsList) {
    this.ownerPermissionsList = ownerPermissionsList;
  }

}
