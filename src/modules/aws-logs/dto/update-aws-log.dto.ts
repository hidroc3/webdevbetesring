import { PartialType } from '@nestjs/mapped-types';
import { CreateAwsLogDto } from './create-aws-log.dto';

export class UpdateAwsLogDto extends PartialType(CreateAwsLogDto) {}
