import { PartialType } from '@nestjs/mapped-types';
import { CreatePostHydrologicDto } from './create-post-hydrologic.dto';

export class UpdatePostHydrologicDto extends PartialType(
  CreatePostHydrologicDto,
) {}
