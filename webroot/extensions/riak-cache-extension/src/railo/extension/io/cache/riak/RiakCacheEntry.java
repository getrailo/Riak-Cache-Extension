package railo.extension.io.cache.riak;

import java.util.Date;

import railo.commons.io.cache.CacheEntry;
import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.exp.PageException;
import railo.runtime.type.Struct;

public class RiakCacheEntry implements CacheEntry {
	
	private RiakDocument doc;
	
	public RiakCacheEntry(RiakDocument doc) {
		this.doc = doc;
	}

	@Override
	public Date created() {
		try{
			Date created = new Date(new Long(this.doc.getData().get("created").toString()));	
			return created;
		}catch(PageException e){
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Struct getCustomInfo() {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Struct str = engine.getCreationUtil().createStruct();
		return str;
	}

	@Override
	public String getKey() {
		return this.doc.getKey();
	}

	@Override
	public Object getValue() {
		return this.doc.getValue();
	}

	@Override
	public int hitCount() {
		try {
			int hits = Integer.parseInt(this.doc.getData().get("hits").toString());
			return hits;
		} catch (PageException e) {
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public long idleTimeSpan() {
		Long timeidle = System.currentTimeMillis();
		try{
			timeidle = new Long(this.doc.getData().get("timeIdle").toString());	
			
		}catch(PageException e){
			e.printStackTrace();
		}
		return timeidle;
	}

	@Override
	public Date lastHit() {
		return lastModified();	
	}

	@Override
	public Date lastModified() {
		try{
			Date lastModified = new Date(new Long(this.doc.getData().get("lastModified").toString()));	
			return lastModified;
		}catch(PageException e){
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public long liveTimeSpan() {
		Long expires = System.currentTimeMillis();
		try{
			expires = new Long(this.doc.getData().get("expires").toString());	
			
		}catch(PageException e){
			e.printStackTrace();
		}
		return expires;
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
