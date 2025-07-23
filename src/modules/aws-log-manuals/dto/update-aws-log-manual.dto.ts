import { PartialType } from '@nestjs/mapped-types';
import { CreateAwsLogManualDto } from './create-aws-log-manual.dto';

export class UpdateAwsLogManualDto extends PartialType(CreateAwsLogManualDto) {}
