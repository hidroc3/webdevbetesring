import { PartialType } from '@nestjs/mapped-types';
import { CreateDasDto } from './create-das.dto';

export class UpdateDasDto extends PartialType(CreateDasDto) {}
