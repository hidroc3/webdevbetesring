import { PartialType } from '@nestjs/mapped-types';
import { CreateAwsStationDto } from './create-aws-station.dto';

export class UpdateAwsStationDto extends PartialType(CreateAwsStationDto) {}
