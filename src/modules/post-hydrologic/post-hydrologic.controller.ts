import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { PostHydrologicService } from './post-hydrologic.service';
import { CreatePostHydrologicDto } from './dto/create-post-hydrologic.dto';
import { UpdatePostHydrologicDto } from './dto/update-post-hydrologic.dto';

@Controller('post-hydrologic')
export class PostHydrologicController {
  constructor(private readonly service: PostHydrologicService) {}

  @Post()
  async create(@Body() dto: CreatePostHydrologicDto) {
    const data = await this.service.create(dto);
    return { message: 'Post Hydrologic created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'All Post Hydrologic data retrieved', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return { message: 'Post Hydrologic detail retrieved', data };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdatePostHydrologicDto) {
    const data = await this.service.update(+id, dto);
    return { message: 'Post Hydrologic updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Post Hydrologic deleted successfully', data };
  }
}
