import { PartialType } from '@nestjs/mapped-types';
import { CreateReportDetailDto } from './create-report-detail.dto';

export class UpdateReportDetailDto extends PartialType(CreateReportDetailDto) {}
