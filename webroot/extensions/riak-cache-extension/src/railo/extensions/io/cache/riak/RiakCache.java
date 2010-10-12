package railo.extensions.io.cache.riak;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;

import railo.commons.io.cache.Cache;
import railo.commons.io.cache.CacheEntry;
import railo.commons.io.cache.CacheEntryFilter;
import railo.commons.io.cache.CacheKeyFilter;
import railo.extension.io.util.Functions;
import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.exp.PageException;
import railo.runtime.type.Struct;
import railo.runtime.util.Cast;

public class RiakCache implements Cache {
	
	private String cacheName;
	private String host;
	private RiakClient rc;
	private Functions func = new Functions();
	
	//counters
	private int hits = 0;
	private int misses = 0;
	
	
	@Override
	public void init(String cacheName, Struct args) throws IOException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		
		this.cacheName = cacheName;
		
		try{
			this.host = "http://" + caster.toString(args.get("host")) + "/riak";
		}catch (PageException e) {
			e.printStackTrace();
		}
		
		
		RiakConfig config = new RiakConfig(this.host);
		this.rc = new RiakClient(config);
		
	}

	public RiakClient init(String cacheName) throws IOException {
		
		this.cacheName = cacheName;
		
		RiakConfig config = new RiakConfig("http://localhost:8098/riak");	
		//client
		return new RiakClient(config);
		
	}
	
	@Override
	public boolean contains(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List entries() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List entries(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List entries(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheEntry getCacheEntry(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheEntry getCacheEntry(String arg0, CacheEntry arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct getCustomInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long hitCount() {
		return this.hits;
	}

	@Override
	public List keys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List keys(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List keys(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long missCount() {
		return this.misses;
	}

	@Override
	public void put(String key, Object value, Long idleTime, Long lifeSpan) {
		long created = System.currentTimeMillis();
		long idle = idleTime==null?0:idleTime.longValue();
		long life = lifeSpan==null?0:lifeSpan.longValue();
		
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Struct doc = engine.getCreationUtil().createStruct();
		
	}

	@Override
	public boolean remove(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int remove(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int remove(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List values(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List values(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}