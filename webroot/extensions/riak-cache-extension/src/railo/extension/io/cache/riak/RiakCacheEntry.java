package railo.extension.io.cache.riak;

import java.util.Date;

import railo.commons.io.cache.CacheEntry;
import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.exp.PageException;
import railo.runtime.type.Struct;
import railo.runtime.util.Cast;

public class RiakCacheEntry implements CacheEntry {
	
	private RiakDocument doc;
	
	public RiakCacheEntry(RiakDocument doc) {
		this.doc = doc;
	}

	@Override
	public Date created() {
		try{
			CFMLEngine engine = CFMLEngineFactory.getInstance();
			Cast caster = engine.getCastUtil();
			Date created = new Date(caster.toLongValue((this.doc.getData().get("created"))));	
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
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		try {
			int hits = caster.toIntValue(this.doc.getData().get("hits"));
			return hits;
		} catch (PageException e) {
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public long idleTimeSpan() {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		Long timeidle = System.currentTimeMillis();
		try{
			timeidle = caster.toLongValue(this.doc.getData().get("timeIdle"));				
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
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		try{
			Date lastModified = new Date(caster.toLongValue((this.doc.getData().get("lastHit"))));	
			return lastModified;
		}catch(PageException e){
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public long liveTimeSpan() {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		Long expires = System.currentTimeMillis();
		try{
			expires = caster.toLongValue(this.doc.getData().get("expires"));		
			
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
