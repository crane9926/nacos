/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.naming.utils;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.selector.ExpressionSelector;
import com.alibaba.nacos.api.selector.NoneSelector;
import com.alibaba.nacos.api.selector.SelectorType;
import com.alibaba.nacos.client.utils.*;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * @author liaochuntao
 * @author deshao
 */
public class InitUtils {

    /**
     * Add a difference to the name naming. This method simply initializes the namespace for Naming.
     * Config initialization is not the same, so it cannot be reused directly.
     *初始化NameSpace
     * @param properties
     * @return
     */
    public static String initNamespaceForNaming(Properties properties) {
        String tmpNamespace = null;
        //传入的properties会指定是否解析云环境中的namespace参数，如果是的，就是去读取阿里云环境的系统变量；
        // 如果不是，那么就读取properties中指定的namespace，没有指定的话，最终解析出来的是空字符串。
        String isUseCloudNamespaceParsing =
            properties.getProperty(PropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                System.getProperty(SystemPropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                    String.valueOf(Constants.DEFAULT_USE_CLOUD_NAMESPACE_PARSING)));

        //如果是云上环境
        if (Boolean.parseBoolean(isUseCloudNamespaceParsing)) {

            tmpNamespace = TenantUtil.getUserTenantForAns();
            //从系统变量获取ans.namespace
            tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
                @Override
                public String call() {
                    String namespace = System.getProperty(SystemPropertyKeyConst.ANS_NAMESPACE);
                    LogUtils.NAMING_LOGGER.info("initializer namespace from System Property :" + namespace);
                    return namespace;
                }
            });
            //从系统变量获取ALIBABA_ALIWARE_NAMESPACE
            tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
                @Override
                public String call() {
                    String namespace = System.getenv(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_NAMESPACE);
                    LogUtils.NAMING_LOGGER.info("initializer namespace from System Environment :" + namespace);
                    return namespace;
                }
            });
        }

        //如果不是云上环境，那么从系统变量获取namespace
        tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
            @Override
            public String call() {
                String namespace = System.getProperty(PropertyKeyConst.NAMESPACE);
                LogUtils.NAMING_LOGGER.info("initializer namespace from System Property :" + namespace);
                return namespace;
            }
        });

        //如果上面都没有获取到，则从properties中获取namespace
        if (StringUtils.isEmpty(tmpNamespace) && properties != null) {
            tmpNamespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        }

        //如果还没查到，则获取系统默认的namespace即public
        tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
            @Override
            public String call() {
                return UtilAndComs.DEFAULT_NAMESPACE_ID;
            }
        });
        return tmpNamespace;
    }

    public static void initWebRootContext() {
        // support the web context with ali-yun if the app deploy by EDAS
        //支持阿里云上的webContext
        final String webContext = System.getProperty(SystemPropertyKeyConst.NAMING_WEB_CONTEXT);

        TemplateUtils.stringNotEmptyAndThenExecute(webContext, new Runnable() {
            @Override
            public void run() {
                UtilAndComs.WEB_CONTEXT = webContext.indexOf("/") > -1 ? webContext
                    : "/" + webContext;

                UtilAndComs.NACOS_URL_BASE = UtilAndComs.WEB_CONTEXT + "/v1/ns";
                UtilAndComs.NACOS_URL_INSTANCE = UtilAndComs.NACOS_URL_BASE + "/instance";
            }
        });
    }

    public static String initEndpoint(final Properties properties) {
        if (properties == null) {

            return "";
        }
        // Whether to enable domain name resolution rules
        //是否使用endpoint解析，默认为true。getProperty方法的第二个参数即为默认值。
        String isUseEndpointRuleParsing =
            properties.getProperty(PropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                System.getProperty(SystemPropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                    String.valueOf(ParamUtil.USE_ENDPOINT_PARSING_RULE_DEFAULT_VALUE)));

        boolean isUseEndpointParsingRule = Boolean.parseBoolean(isUseEndpointRuleParsing);
        String endpointUrl;
        //使用endpoint解析功能
        if (isUseEndpointParsingRule) {
            // Get the set domain name information
            endpointUrl = ParamUtil.parsingEndpointRule(properties.getProperty(PropertyKeyConst.ENDPOINT));
            if (StringUtils.isBlank(endpointUrl)) {
                return "";
            }
        } else {
            //如果不适用endpoint，直接通过properties文件来获取
            endpointUrl = properties.getProperty(PropertyKeyConst.ENDPOINT);
        }

        if (StringUtils.isBlank(endpointUrl)) {
            return "";
        }

        String endpointPort = TemplateUtils.stringEmptyAndThenExecute(System.getenv(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_PORT), new Callable<String>() {
            @Override
            public String call() {

                return properties.getProperty(PropertyKeyConst.ENDPOINT_PORT);
            }
        });

        endpointPort = TemplateUtils.stringEmptyAndThenExecute(endpointPort, new Callable<String>() {
            @Override
            public String call() {
                return "8080";
            }
        });

        return endpointUrl + ":" + endpointPort;
    }

    /**
     * Register subType for serialization
     *
     * Now these subType implementation class has registered in static code.
     * But there are some problem for classloader. The implementation class
     * will be loaded when they are used, which will make deserialize
     * before register.
     *
     * 子类实现类中的静态代码串中已经向Jackson进行了注册，但是由于classloader的原因，只有当
     * 该子类被使用的时候，才会加载该类。这可能会导致Jackson先进性反序列化，再注册子类，从而导致
     * 反序列化失败。
     */
    public static void initSerialization() {
        // TODO register in implementation class or remove subType
        JacksonUtils.registerSubtype(NoneSelector.class, SelectorType.none.name());
        JacksonUtils.registerSubtype(ExpressionSelector.class, SelectorType.label.name());
    }
}
