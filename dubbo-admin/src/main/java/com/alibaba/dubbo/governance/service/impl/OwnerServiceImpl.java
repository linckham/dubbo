package com.alibaba.dubbo.governance.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.governance.service.OverrideService;
import com.alibaba.dubbo.governance.service.OwnerService;
import com.alibaba.dubbo.governance.service.ProviderService;
import com.alibaba.dubbo.registry.common.domain.Override;
import com.alibaba.dubbo.registry.common.domain.Owner;
import com.alibaba.dubbo.registry.common.domain.Provider;

public class OwnerServiceImpl extends AbstractService implements OwnerService {
    
    @Autowired
    ProviderService providerService;
    
    @Autowired
    OverrideService overrideService;

    public List<String> findAllServiceNames() {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> findServiceNamesByUsername(String username) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> findUsernamesByServiceName(String serviceName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Owner> findByService(String serviceName) {
        List<Provider> pList = providerService.findByService(serviceName);
        List<Override> cList = overrideService.findByServiceAndAddress(serviceName, Constants.ANYHOST_VALUE);
        return toOverrideList(pList, cList);
    }

    public List<Owner> findAll() {
        List<Provider> pList = providerService.findAll();
        List<Override> cList = overrideService.findAll();
        return toOverrideList(pList, cList);
    }

    public Owner findById(Long id) {
       
        return null;
    }
    
    private List<Owner> toOverrideList(List<Provider> pList, List<Override> cList){
        Map<String, Owner> oList = new HashMap<String, Owner>();
        if(null != pList && !pList.isEmpty()) {
            for (Provider p : pList) {
                if (p.getUsername() != null) {
                    for (String username : Constants.COMMA_SPLIT_PATTERN.split(p.getUsername())) {
                        Owner o = new Owner();
                        o.setService(p.getService());
                        o.setUsername(username);
                        oList.put(o.getService() + "/" + o.getUsername(), o);
                    }
                }
            }
        }
        if(null !=cList && !cList.isEmpty()) {
            for (Override c : cList) {
                Map<String, String> params = StringUtils.parseQueryString(c.getParams());
                String userNames = params.get("owner");
                if (userNames != null && userNames.length() > 0) {
                    for (String username : Constants.COMMA_SPLIT_PATTERN.split(userNames)) {
                        Owner o = new Owner();
                        o.setService(c.getService());
                        o.setUsername(username);
                        oList.put(o.getService() + "/" + o.getUsername(), o);
                    }
                }
            }
        }
        return new ArrayList<Owner>(oList.values());
    }

	public void saveOwner(Owner owner) {
		List<Override> overrides = overrideService.findByServiceAndAddress(owner.getService(), Constants.ANYHOST_VALUE);
        if (overrides == null || overrides.size() == 0) {
        	Override override = new Override();
        	override.setAddress(Constants.ANYHOST_VALUE);
        	override.setService(owner.getService());
        	override.setEnabled(true);
        	override.setParams("owner=" + owner.getUsername());
        	overrideService.saveOverride(override);
        } else {
	        for(Override override : overrides){
	        	Map<String, String> params = StringUtils.parseQueryString(override.getParams());
	        	String userNames = params.get("owner");
	        	if (userNames == null || userNames.length() == 0) {
	        		userNames = owner.getUsername();
	        	} else {
	        		userNames = userNames + "," + owner.getUsername();
	        	}
	        	params.put("owner", userNames);
	        	override.setParams(StringUtils.toQueryString(params));
        		overrideService.updateOverride(override);
	        }
        }
	}

	public void deleteOwner(Owner owner) {
		List<Override> overrides = overrideService.findByServiceAndAddress(owner.getService(), Constants.ANYHOST_VALUE);
        if (overrides == null || overrides.size() == 0) {
        	Override override = new Override();
        	override.setAddress(Constants.ANYHOST_VALUE);
        	override.setService(owner.getService());
        	override.setEnabled(true);
        	override.setParams("owner=" + owner.getUsername());
        	overrideService.saveOverride(override);
        } else {
	        for(Override override : overrides){
	        	Map<String, String> params = StringUtils.parseQueryString(override.getParams());
	        	String userNames = params.get("owner");
	        	if (userNames != null && userNames.length() > 0) {
	        		if (userNames.equals(owner.getUsername())) {
	        			params.remove("owner");
	        		} else {
	        			userNames = userNames.replace(owner.getUsername() + ",", "").replace("," + owner.getUsername(), "");
	        			params.put("owner", userNames);
	        		}
	        		if (params.size() > 0) {
		        		override.setParams(StringUtils.toQueryString(params));
		        		overrideService.updateOverride(override);
		        	} else {
		        		overrideService.deleteOverride(override.getId());
		        	}
	        	}
	        }
        }
	}

}
