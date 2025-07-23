import { PartialType } from '@nestjs/mapped-types';
import { CreateSubDistrictDto } from './create-sub-district.dto';

export class UpdateSubDistrictDto extends PartialType(CreateSubDistrictDto) {}
