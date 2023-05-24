package org.apache.hudi.common.config;

public interface HoodieConfiguration<Conf> {
  void setConf(Conf conf);
}
