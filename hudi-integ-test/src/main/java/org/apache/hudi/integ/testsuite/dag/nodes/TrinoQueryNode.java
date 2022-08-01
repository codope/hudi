package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TrinoQueryNode extends BaseQueryNode{

  public TrinoQueryNode(DeltaConfig.Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext context, int curItrCount) throws Exception {
    log.info("Executing trino query node {}", this.getName());
    String url = context.getHoodieTestSuiteWriter().getCfg().trinoJdbcUrl;
    if (StringUtils.isNullOrEmpty(url)) {
      throw new IllegalArgumentException("Trino JDBC connection url not provided. Please set --trino-jdbc-url.");
    }
    String user = context.getHoodieTestSuiteWriter().getCfg().trinoUsername;
    String pass = context.getHoodieTestSuiteWriter().getCfg().trinoPassword;
    try {
      Class.forName("io.trino.jdbc.TrinoDriver");
    } catch (ClassNotFoundException e) {
      throw new HoodieValidationException("Trino query validation failed due to " + e.getMessage(), e);
    }
    try (Connection connection = DriverManager.getConnection(url, user, pass)) {
      Statement stmt = connection.createStatement();
      setSessionProperties(this.config.getTrinoProperties(), stmt);
      executeAndValidateQueries(this.config.getTrinoQueries(), stmt);
      stmt.close();
    }
    catch (Exception e) {
      throw new HoodieValidationException("Trino query validation failed due to " + e.getMessage(), e);
    }
  }
}
