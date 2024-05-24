﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sagemaker/SageMaker_EXPORTS.h>
#include <aws/sagemaker/model/S3StorageConfig.h>
#include <aws/sagemaker/model/DataCatalogConfig.h>
#include <aws/sagemaker/model/TableFormat.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Json
{
  class JsonValue;
  class JsonView;
} // namespace Json
} // namespace Utils
namespace SageMaker
{
namespace Model
{

  /**
   * <p>The configuration of an <code>OfflineStore</code>.</p> <p>Provide an
   * <code>OfflineStoreConfig</code> in a request to <code>CreateFeatureGroup</code>
   * to create an <code>OfflineStore</code>.</p> <p>To encrypt an
   * <code>OfflineStore</code> using at rest data encryption, specify Amazon Web
   * Services Key Management Service (KMS) key ID, or <code>KMSKeyId</code>, in
   * <code>S3StorageConfig</code>.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sagemaker-2017-07-24/OfflineStoreConfig">AWS
   * API Reference</a></p>
   */
  class OfflineStoreConfig
  {
  public:
    AWS_SAGEMAKER_API OfflineStoreConfig();
    AWS_SAGEMAKER_API OfflineStoreConfig(Aws::Utils::Json::JsonView jsonValue);
    AWS_SAGEMAKER_API OfflineStoreConfig& operator=(Aws::Utils::Json::JsonView jsonValue);
    AWS_SAGEMAKER_API Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The Amazon Simple Storage (Amazon S3) location of
     * <code>OfflineStore</code>.</p>
     */
    inline const S3StorageConfig& GetS3StorageConfig() const{ return m_s3StorageConfig; }

    /**
     * <p>The Amazon Simple Storage (Amazon S3) location of
     * <code>OfflineStore</code>.</p>
     */
    inline bool S3StorageConfigHasBeenSet() const { return m_s3StorageConfigHasBeenSet; }

    /**
     * <p>The Amazon Simple Storage (Amazon S3) location of
     * <code>OfflineStore</code>.</p>
     */
    inline void SetS3StorageConfig(const S3StorageConfig& value) { m_s3StorageConfigHasBeenSet = true; m_s3StorageConfig = value; }

    /**
     * <p>The Amazon Simple Storage (Amazon S3) location of
     * <code>OfflineStore</code>.</p>
     */
    inline void SetS3StorageConfig(S3StorageConfig&& value) { m_s3StorageConfigHasBeenSet = true; m_s3StorageConfig = std::move(value); }

    /**
     * <p>The Amazon Simple Storage (Amazon S3) location of
     * <code>OfflineStore</code>.</p>
     */
    inline OfflineStoreConfig& WithS3StorageConfig(const S3StorageConfig& value) { SetS3StorageConfig(value); return *this;}

    /**
     * <p>The Amazon Simple Storage (Amazon S3) location of
     * <code>OfflineStore</code>.</p>
     */
    inline OfflineStoreConfig& WithS3StorageConfig(S3StorageConfig&& value) { SetS3StorageConfig(std::move(value)); return *this;}


    /**
     * <p>Set to <code>True</code> to disable the automatic creation of an Amazon Web
     * Services Glue table when configuring an <code>OfflineStore</code>. If set to
     * <code>False</code>, Feature Store will name the <code>OfflineStore</code> Glue
     * table following <a
     * href="https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html">Athena's
     * naming recommendations</a>.</p> <p>The default value is <code>False</code>.</p>
     */
    inline bool GetDisableGlueTableCreation() const{ return m_disableGlueTableCreation; }

    /**
     * <p>Set to <code>True</code> to disable the automatic creation of an Amazon Web
     * Services Glue table when configuring an <code>OfflineStore</code>. If set to
     * <code>False</code>, Feature Store will name the <code>OfflineStore</code> Glue
     * table following <a
     * href="https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html">Athena's
     * naming recommendations</a>.</p> <p>The default value is <code>False</code>.</p>
     */
    inline bool DisableGlueTableCreationHasBeenSet() const { return m_disableGlueTableCreationHasBeenSet; }

    /**
     * <p>Set to <code>True</code> to disable the automatic creation of an Amazon Web
     * Services Glue table when configuring an <code>OfflineStore</code>. If set to
     * <code>False</code>, Feature Store will name the <code>OfflineStore</code> Glue
     * table following <a
     * href="https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html">Athena's
     * naming recommendations</a>.</p> <p>The default value is <code>False</code>.</p>
     */
    inline void SetDisableGlueTableCreation(bool value) { m_disableGlueTableCreationHasBeenSet = true; m_disableGlueTableCreation = value; }

    /**
     * <p>Set to <code>True</code> to disable the automatic creation of an Amazon Web
     * Services Glue table when configuring an <code>OfflineStore</code>. If set to
     * <code>False</code>, Feature Store will name the <code>OfflineStore</code> Glue
     * table following <a
     * href="https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html">Athena's
     * naming recommendations</a>.</p> <p>The default value is <code>False</code>.</p>
     */
    inline OfflineStoreConfig& WithDisableGlueTableCreation(bool value) { SetDisableGlueTableCreation(value); return *this;}


    /**
     * <p>The meta data of the Glue table that is autogenerated when an
     * <code>OfflineStore</code> is created. </p>
     */
    inline const DataCatalogConfig& GetDataCatalogConfig() const{ return m_dataCatalogConfig; }

    /**
     * <p>The meta data of the Glue table that is autogenerated when an
     * <code>OfflineStore</code> is created. </p>
     */
    inline bool DataCatalogConfigHasBeenSet() const { return m_dataCatalogConfigHasBeenSet; }

    /**
     * <p>The meta data of the Glue table that is autogenerated when an
     * <code>OfflineStore</code> is created. </p>
     */
    inline void SetDataCatalogConfig(const DataCatalogConfig& value) { m_dataCatalogConfigHasBeenSet = true; m_dataCatalogConfig = value; }

    /**
     * <p>The meta data of the Glue table that is autogenerated when an
     * <code>OfflineStore</code> is created. </p>
     */
    inline void SetDataCatalogConfig(DataCatalogConfig&& value) { m_dataCatalogConfigHasBeenSet = true; m_dataCatalogConfig = std::move(value); }

    /**
     * <p>The meta data of the Glue table that is autogenerated when an
     * <code>OfflineStore</code> is created. </p>
     */
    inline OfflineStoreConfig& WithDataCatalogConfig(const DataCatalogConfig& value) { SetDataCatalogConfig(value); return *this;}

    /**
     * <p>The meta data of the Glue table that is autogenerated when an
     * <code>OfflineStore</code> is created. </p>
     */
    inline OfflineStoreConfig& WithDataCatalogConfig(DataCatalogConfig&& value) { SetDataCatalogConfig(std::move(value)); return *this;}


    /**
     * <p>Format for the offline store table. Supported formats are Glue (Default) and
     * <a href="https://iceberg.apache.org/">Apache Iceberg</a>.</p>
     */
    inline const TableFormat& GetTableFormat() const{ return m_tableFormat; }

    /**
     * <p>Format for the offline store table. Supported formats are Glue (Default) and
     * <a href="https://iceberg.apache.org/">Apache Iceberg</a>.</p>
     */
    inline bool TableFormatHasBeenSet() const { return m_tableFormatHasBeenSet; }

    /**
     * <p>Format for the offline store table. Supported formats are Glue (Default) and
     * <a href="https://iceberg.apache.org/">Apache Iceberg</a>.</p>
     */
    inline void SetTableFormat(const TableFormat& value) { m_tableFormatHasBeenSet = true; m_tableFormat = value; }

    /**
     * <p>Format for the offline store table. Supported formats are Glue (Default) and
     * <a href="https://iceberg.apache.org/">Apache Iceberg</a>.</p>
     */
    inline void SetTableFormat(TableFormat&& value) { m_tableFormatHasBeenSet = true; m_tableFormat = std::move(value); }

    /**
     * <p>Format for the offline store table. Supported formats are Glue (Default) and
     * <a href="https://iceberg.apache.org/">Apache Iceberg</a>.</p>
     */
    inline OfflineStoreConfig& WithTableFormat(const TableFormat& value) { SetTableFormat(value); return *this;}

    /**
     * <p>Format for the offline store table. Supported formats are Glue (Default) and
     * <a href="https://iceberg.apache.org/">Apache Iceberg</a>.</p>
     */
    inline OfflineStoreConfig& WithTableFormat(TableFormat&& value) { SetTableFormat(std::move(value)); return *this;}

  private:

    S3StorageConfig m_s3StorageConfig;
    bool m_s3StorageConfigHasBeenSet = false;

    bool m_disableGlueTableCreation;
    bool m_disableGlueTableCreationHasBeenSet = false;

    DataCatalogConfig m_dataCatalogConfig;
    bool m_dataCatalogConfigHasBeenSet = false;

    TableFormat m_tableFormat;
    bool m_tableFormatHasBeenSet = false;
  };

} // namespace Model
} // namespace SageMaker
} // namespace Aws