import { Test, TestingModule } from '@nestjs/testing';
import { HydrologicOutputHechmsService } from './hydrologic-output-hechms.service';

describe('HydrologicOutputHechmsService', () => {
  let service: HydrologicOutputHechmsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [HydrologicOutputHechmsService],
    }).compile();

    service = module.get<HydrologicOutputHechmsService>(HydrologicOutputHechmsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
