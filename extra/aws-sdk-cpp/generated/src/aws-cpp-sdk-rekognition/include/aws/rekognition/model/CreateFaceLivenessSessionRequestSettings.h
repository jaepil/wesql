﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/rekognition/Rekognition_EXPORTS.h>
#include <aws/rekognition/model/LivenessOutputConfig.h>
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
namespace Rekognition
{
namespace Model
{

  /**
   * <p>A session settings object. It contains settings for the operation to be
   * performed. It accepts arguments for OutputConfig and
   * AuditImagesLimit.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/rekognition-2016-06-27/CreateFaceLivenessSessionRequestSettings">AWS
   * API Reference</a></p>
   */
  class CreateFaceLivenessSessionRequestSettings
  {
  public:
    AWS_REKOGNITION_API CreateFaceLivenessSessionRequestSettings();
    AWS_REKOGNITION_API CreateFaceLivenessSessionRequestSettings(Aws::Utils::Json::JsonView jsonValue);
    AWS_REKOGNITION_API CreateFaceLivenessSessionRequestSettings& operator=(Aws::Utils::Json::JsonView jsonValue);
    AWS_REKOGNITION_API Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>Can specify the location of an Amazon S3 bucket, where reference and audit
     * images will be stored. Note that the Amazon S3 bucket must be located in the
     * caller's AWS account and in the same region as the Face Liveness end-point.
     * Additionally, the Amazon S3 object keys are auto-generated by the Face Liveness
     * system. Requires that the caller has the <code>s3:PutObject</code> permission on
     * the Amazon S3 bucket.</p>
     */
    inline const LivenessOutputConfig& GetOutputConfig() const{ return m_outputConfig; }

    /**
     * <p>Can specify the location of an Amazon S3 bucket, where reference and audit
     * images will be stored. Note that the Amazon S3 bucket must be located in the
     * caller's AWS account and in the same region as the Face Liveness end-point.
     * Additionally, the Amazon S3 object keys are auto-generated by the Face Liveness
     * system. Requires that the caller has the <code>s3:PutObject</code> permission on
     * the Amazon S3 bucket.</p>
     */
    inline bool OutputConfigHasBeenSet() const { return m_outputConfigHasBeenSet; }

    /**
     * <p>Can specify the location of an Amazon S3 bucket, where reference and audit
     * images will be stored. Note that the Amazon S3 bucket must be located in the
     * caller's AWS account and in the same region as the Face Liveness end-point.
     * Additionally, the Amazon S3 object keys are auto-generated by the Face Liveness
     * system. Requires that the caller has the <code>s3:PutObject</code> permission on
     * the Amazon S3 bucket.</p>
     */
    inline void SetOutputConfig(const LivenessOutputConfig& value) { m_outputConfigHasBeenSet = true; m_outputConfig = value; }

    /**
     * <p>Can specify the location of an Amazon S3 bucket, where reference and audit
     * images will be stored. Note that the Amazon S3 bucket must be located in the
     * caller's AWS account and in the same region as the Face Liveness end-point.
     * Additionally, the Amazon S3 object keys are auto-generated by the Face Liveness
     * system. Requires that the caller has the <code>s3:PutObject</code> permission on
     * the Amazon S3 bucket.</p>
     */
    inline void SetOutputConfig(LivenessOutputConfig&& value) { m_outputConfigHasBeenSet = true; m_outputConfig = std::move(value); }

    /**
     * <p>Can specify the location of an Amazon S3 bucket, where reference and audit
     * images will be stored. Note that the Amazon S3 bucket must be located in the
     * caller's AWS account and in the same region as the Face Liveness end-point.
     * Additionally, the Amazon S3 object keys are auto-generated by the Face Liveness
     * system. Requires that the caller has the <code>s3:PutObject</code> permission on
     * the Amazon S3 bucket.</p>
     */
    inline CreateFaceLivenessSessionRequestSettings& WithOutputConfig(const LivenessOutputConfig& value) { SetOutputConfig(value); return *this;}

    /**
     * <p>Can specify the location of an Amazon S3 bucket, where reference and audit
     * images will be stored. Note that the Amazon S3 bucket must be located in the
     * caller's AWS account and in the same region as the Face Liveness end-point.
     * Additionally, the Amazon S3 object keys are auto-generated by the Face Liveness
     * system. Requires that the caller has the <code>s3:PutObject</code> permission on
     * the Amazon S3 bucket.</p>
     */
    inline CreateFaceLivenessSessionRequestSettings& WithOutputConfig(LivenessOutputConfig&& value) { SetOutputConfig(std::move(value)); return *this;}


    /**
     * <p>Number of audit images to be returned back. Takes an integer between 0-4. Any
     * integer less than 0 will return 0, any integer above 4 will return 4 images in
     * the response. By default, it is set to 0. The limit is best effort and is based
     * on the actual duration of the selfie-video.</p>
     */
    inline int GetAuditImagesLimit() const{ return m_auditImagesLimit; }

    /**
     * <p>Number of audit images to be returned back. Takes an integer between 0-4. Any
     * integer less than 0 will return 0, any integer above 4 will return 4 images in
     * the response. By default, it is set to 0. The limit is best effort and is based
     * on the actual duration of the selfie-video.</p>
     */
    inline bool AuditImagesLimitHasBeenSet() const { return m_auditImagesLimitHasBeenSet; }

    /**
     * <p>Number of audit images to be returned back. Takes an integer between 0-4. Any
     * integer less than 0 will return 0, any integer above 4 will return 4 images in
     * the response. By default, it is set to 0. The limit is best effort and is based
     * on the actual duration of the selfie-video.</p>
     */
    inline void SetAuditImagesLimit(int value) { m_auditImagesLimitHasBeenSet = true; m_auditImagesLimit = value; }

    /**
     * <p>Number of audit images to be returned back. Takes an integer between 0-4. Any
     * integer less than 0 will return 0, any integer above 4 will return 4 images in
     * the response. By default, it is set to 0. The limit is best effort and is based
     * on the actual duration of the selfie-video.</p>
     */
    inline CreateFaceLivenessSessionRequestSettings& WithAuditImagesLimit(int value) { SetAuditImagesLimit(value); return *this;}

  private:

    LivenessOutputConfig m_outputConfig;
    bool m_outputConfigHasBeenSet = false;

    int m_auditImagesLimit;
    bool m_auditImagesLimitHasBeenSet = false;
  };

} // namespace Model
} // namespace Rekognition
} // namespace Aws