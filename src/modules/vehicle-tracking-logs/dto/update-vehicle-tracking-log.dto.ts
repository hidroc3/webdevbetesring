import { PartialType } from '@nestjs/mapped-types';
import { CreateVehicleTrackingLogDto } from './create-vehicle-tracking-log.dto';

export class UpdateVehicleTrackingLogDto extends PartialType(
  CreateVehicleTrackingLogDto,
) {}
